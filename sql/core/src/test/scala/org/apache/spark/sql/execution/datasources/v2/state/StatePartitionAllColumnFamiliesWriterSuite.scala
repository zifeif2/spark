/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.v2.state

import org.apache.spark.sql.execution.streaming.checkpointing.{CommitMetadata}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryCheckpointMetadata}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.execution.streaming.utils.StreamingUtils
import org.apache.spark.sql.functions.{count, max, min, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode

/**
 * Test suite for StatePartitionAllColumnFamiliesWriter.
 * Tests the writer's ability to correctly write raw bytes read from
 * StatePartitionAllColumnFamiliesReader to a new state store location.
 */
class StatePartitionAllColumnFamiliesWriterSuite extends StateDataSourceTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      classOf[RocksDBStateStoreProvider].getName)
  }

  test("round-trip: read raw bytes and write to new location") {
    withSQLConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> "2",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTempDir { sourceDir =>
        withTempDir { targetDir =>
          import testImplicits._
          // Step 1: Create state by running a streaming aggregation
          runLargeDataStreamingAggregationQuery(sourceDir.getAbsolutePath)
          val inputData: MemoryStream[Int] = MemoryStream[Int]
          val aggregated = inputData.toDF()
            .selectExpr("value", "value % 10 AS groupKey")
            .groupBy("groupKey")
            .agg(
              count("*").as("cnt"),
              sum("value").as("sum"),
              max("value").as("max"),
              min("value").as("min")
            )
            .as[(Int, Long, Long, Int, Int)]

          // check with more data - leverage full partitions
          testStream(aggregated, OutputMode.Update)(
            StartStream(checkpointLocation = targetDir.getAbsolutePath),
            // batch 0
            AddData(inputData, 0 until 2: _*),
              CheckLastBatch(
                (0, 1, 0, 0, 0), // 0
                (1, 1, 1, 1, 1) // 1
              ),
            AddData(inputData, 0 until 2: _*),
            CheckLastBatch(
              (0, 2, 0, 0, 0), // 0
              (1, 2, 2, 1, 1) // 1
            ),
              // batch 1

            // batch 2
            AddData(inputData, 0 until 1: _*),
            CheckLastBatch(
              (0, 3, 0, 0, 0) // 0
            )
          )


          // Step 2: Read original state using normal reader (for comparison later)"
          val sourceNormalData = spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, sourceDir.getAbsolutePath)
            .load()
            .selectExpr("key", "value", "partition_id")
            .collect()

//          assert(sourceNormalData.nonEmpty, "Source data should not be empty")
          // Step 3: Read from source using AllColumnFamiliesReader (raw bytes)
          val sourceBytesData = spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, sourceDir.getAbsolutePath)
            .option(StateSourceOptions.INTERNAL_ONLY_READ_ALL_COLUMN_FAMILIES, "true")
            .load()
          sourceBytesData.repartition(3)

          // Verify schema of raw bytes
          val schema = sourceBytesData.schema
          assert(schema.fieldNames === Array(
            "partition_key", "key_bytes", "value_bytes", "column_family_name"))
          // ===== ADD THIS DEBUG CODE =====
          println("\n=== sourceBytesData Schema ===")
          sourceBytesData.printSchema()

          println("\n=== Sample Rows (first 5) ===")
          sourceBytesData.show(5, truncate = false)

          println("\n=== Detailed Row Inspection ===")
          sourceBytesData.limit(2).collect().foreach { row =>
            val partitionKey = row.getStruct(0)
            val keyBytes = row.getAs[Array[Byte]](1)
            val valueBytes = row.getAs[Array[Byte]](2)
            val colFamily = row.getString(3)

            println(s"Column Family: $colFamily")
            println(s"  partition_key: ${partitionKey}")
            println(s"  keyBytes.length: ${keyBytes.length}")
            println(s"  valueBytes.length: ${valueBytes.length}")
            println(s"  valueBytes.length % 8: ${valueBytes.length % 8}")
            println()
          }
          // Step 4: Write raw bytes to target checkpoint location
          val hadoopConf = spark.sessionState.newHadoopConf()
          val resolvedCpLocation = StreamingUtils.resolvedCheckpointLocation(
            hadoopConf, targetDir.getAbsolutePath)
          val checkpointMetadata = new StreamingQueryCheckpointMetadata(
            spark, resolvedCpLocation)  // Add spark as first parameter
          val targetOffsetSeq = checkpointMetadata.offsetLog.get(2).get
          checkpointMetadata.offsetLog.add(3, targetOffsetSeq)
          sourceBytesData.write
            .format("statestore")
            .mode("Append")
            .option(StateSourceOptions.PATH, targetDir.getAbsolutePath)
            .option(StateSourceOptions.INTERNAL_ONLY_READ_ALL_COLUMN_FAMILIES, "true")
            .save()

          // Step 5: Read from target using normal reader
          val targetNormalData = spark.read
            .format("statestore")
            .option(StateSourceOptions.PATH, targetDir.getAbsolutePath)
            .load()
            .selectExpr("key", "value", "partition_id")
            .collect()
          // Step 6: Verify data matches
          assert(sourceNormalData.length == targetNormalData.length,
            s"Row count mismatch: source=${sourceNormalData.length}, " +
              s"target=${targetNormalData.length}")

          // Sort and compare row by row
          val sourceSorted = sourceNormalData.sortBy(_.toString)
          val targetSorted = targetNormalData.sortBy(_.toString)

          sourceSorted.zip(targetSorted).zipWithIndex.foreach {
            case ((sourceRow, targetRow), idx) =>
              assert(sourceRow == targetRow,
                s"Row mismatch at index $idx:\n" +
                  s"  Source: $sourceRow\n" +
                  s"  Target: $targetRow")
          }
        }
      }
    }
  }
}
