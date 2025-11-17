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

import java.sql.Timestamp

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.execution.streaming.state.StateStoreTestsHelper
import org.apache.spark.sql.functions.{col, count, sum, timestamp_seconds}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{ListState, OutputMode, StatefulProcessor, TimeMode}
import org.apache.spark.sql.streaming.{TimerValues, TTLConfig, ValueState}
import org.apache.spark.sql.types.StructType
import org.apache.spark.tags.SlowSQLTest


/**
 * Stateful processor with multiple state variables to create multiple column families.
 */
class MultiStateVariableProcessor
  extends StatefulProcessor[String, String, (String, String)] {

  @transient var _listState: ListState[String] = _
  @transient var _valueState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _listState = getHandle.getListState("testListState", Encoders.STRING, TTLConfig.NONE)
    _valueState = getHandle.getValueState("testValueState", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    // Update list state
    rows.foreach { value =>
      _listState.appendValue(value)
    }

    // Update value state with count
    val count = Option(_valueState.get()).getOrElse(0L) + 1
    _valueState.update(count)

    Iterator((key, count.toString))
  }
}

/**
 * Stateful processor with ProcessingTime timers.
 */
class ProcessingTimeTimerProcessor
  extends StatefulProcessor[String, String, (String, String)] {

  @transient var _valueState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _valueState = getHandle.getValueState("testValueState", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val count = Option(_valueState.get()).getOrElse(0L) + rows.size
    _valueState.update(count)

    // Register a processing time timer
    getHandle.registerTimer(System.currentTimeMillis() + 10000)

    Iterator((key, count.toString))
  }
}

/**
 * Stateful processor with EventTime timers.
 */
class EventTimeTimerProcessor
  extends StatefulProcessor[String, (String, Timestamp), (String, String)] {

  @transient var _valueState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _valueState = getHandle.getValueState("testValueState", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[(String, Timestamp)],
      timerValues: TimerValues): Iterator[(String, String)] = {
    var maxTimestamp = 0L
    var rowCount = 0
    rows.foreach { case (_, timestamp) =>
      maxTimestamp = Math.max(maxTimestamp, timestamp.getTime)
      rowCount += 1
    }

    val count = Option(_valueState.get()).getOrElse(0L) + rowCount
    _valueState.update(count)

    // Register an event time timer
    if (maxTimestamp > 0) {
      getHandle.registerTimer(maxTimestamp + 5000)
    }

    Iterator((key, count.toString))
  }
}

/**
 * Test suite to verify StatePartitionReaderAllColumnFamilies functionality.
 */
@SlowSQLTest
class StatePartitionReaderAllColumnFamiliesSuite extends StateDataSourceTestBase {

  import testImplicits._
  protected val keySchema: StructType = StateStoreTestsHelper.keySchema
  protected val valueSchema: StructType = StateStoreTestsHelper.valueSchema

  /**
   * Helper method to verify column families in state data.
   */
  private def verifyStateData(
      checkpointDir: String,
      expectedRowCount: Int,
      expectedColumnFamilies: Seq[String]): Unit = {
    val stateReadDf = spark.read
      .format("statestore")
      .option(StateSourceOptions.PATH, checkpointDir)
      .option(StateSourceOptions.READ_ALL_COLUMN_FAMILIES, true)
      .load()

    // Verify schema
    val schema = stateReadDf.schema
    assert(schema.fieldNames === Array(
      "partition_id", "key_bytes", "value_bytes", "column_family_name"))
    assert(schema("partition_id").dataType.typeName === "integer")
    assert(schema("key_bytes").dataType.typeName === "binary")
    assert(schema("value_bytes").dataType.typeName === "binary")
    assert(schema("column_family_name").dataType.typeName === "string")

    // Verify data
    val rows = stateReadDf.collect()
    assert(rows.length == expectedRowCount,
      s"Expected $expectedRowCount rows but got: ${rows.length}")

    val columnFamilies = rows.map(r => Option(r.getString(3)).getOrElse("null")).distinct.sorted
    assert(columnFamilies.length == expectedColumnFamilies.length,
      s"Expected ${expectedColumnFamilies.length} column families, " +
        s"but got ${columnFamilies.length}: ${columnFamilies.mkString(", ")}")

    expectedColumnFamilies.foreach { expectedCF =>
      val cfToCheck = if (expectedCF == null) "null" else expectedCF
      assert(columnFamilies.contains(cfToCheck),
        s"Expected column family '$expectedCF', " +
          s"but got: ${columnFamilies.mkString(", ")}")
    }

    // Verify all rows have non-null values
    rows.foreach { row =>
      assert(row.getInt(0) >= 0) // partition_id non-negative
      assert(row.get(1) != null) // key_bytes not null
      assert(row.get(2) != null) // value_bytes not null
    }
  }

  test("read all column families with simple operator") {
    withTempDir { tempDir =>
      withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
          classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {

        val inputData = MemoryStream[Int]
        val aggregated = inputData.toDF()
          .selectExpr("value", "value % 10 AS groupKey")
          .groupBy($"groupKey")
          .agg(
            count("*").as("cnt"),
            sum("value").as("sum")
          )
          .as[(Int, Long, Long)]

        testStream(aggregated, OutputMode.Update)(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          // batch 0
          AddData(inputData, 0 until 20: _*),
          CheckLastBatch(
            (0, 2, 10), // 0, 10
            (1, 2, 12), // 1, 11
            (2, 2, 14), // 2, 12
            (3, 2, 16), // 3, 13
            (4, 2, 18), // 4, 14
            (5, 2, 20), // 5, 15
            (6, 2, 22), // 6, 16
            (7, 2, 24), // 7, 17
            (8, 2, 26), // 8, 18
            (9, 2, 28) // 9, 19
          ),
          StopStream
        )

        // Verify state data - simple aggregation uses default column family (null)
        verifyStateData(tempDir.getAbsolutePath, expectedRowCount = 10,
          expectedColumnFamilies = Seq(null))
      }
    }
  }

  test("read all column families with multiple state variables") {
    withTempDir { tempDir =>
      withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
          classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(
            new MultiStateVariableProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          AddData(inputData, "a", "b", "c"),
          CheckLastBatch(("a", "1"), ("b", "1"), ("c", "1")),
          AddData(inputData, "a", "b"),
          CheckLastBatch(("a", "2"), ("b", "2")),
          StopStream
        )

        // Verify state data
        // 3 keys * (testListState + testValueState + $rowCounter_testListState) = 9 rows
        verifyStateData(
          tempDir.getAbsolutePath,
          expectedRowCount = 9,
          expectedColumnFamilies = Seq(
            "$rowCounter_testListState", "testListState", "testValueState"))
      }
    }
  }
//
//  test("read all column families with processing time timers") {
//    withTempDir { tempDir =>
//      withSQLConf(
//        SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
//          classOf[RocksDBStateStoreProvider].getName,
//        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
//        val inputData = MemoryStream[String]
//        val result = inputData.toDS()
//          .groupByKey(x => x)
//          .transformWithState(
//            new ProcessingTimeTimerProcessor(),
//            TimeMode.ProcessingTime(),
//            OutputMode.Update())
//
//        testStream(result, OutputMode.Update())(
//          StartStream(checkpointLocation = tempDir.getAbsolutePath),
//          AddData(inputData, "a", "b", "c"),
//          CheckLastBatch(("a", "1"), ("b", "1"), ("c", "1")),
//          AddData(inputData, "a", "b"),
//          CheckLastBatch(("a", "2"), ("b", "2")),
//          StopStream
//        )
//
//        // Verify state data includes processing time timer column families
//        // 3 keys * (testValueState + $procTimers_* + $procTimers_*) = 9 rows
//        verifyStateData(
//          tempDir.getAbsolutePath,
//          expectedRowCount = 9,
//          expectedColumnFamilies = Seq(
//            "$procTimers_keyToTimestamp", "$procTimers_timestampToKey", "testValueState"))
//      }
//    }
//  }

  test("read all column families with event time timers") {
    withTempDir { tempDir =>
      withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
          classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        val inputData = MemoryStream[(String, Long)]
        val result = inputData.toDS()
          .select(col("_1").as("key"), timestamp_seconds(col("_2")).as("eventTime"))
          .withWatermark("eventTime", "10 seconds")
          .as[(String, Timestamp)]
          .groupByKey(_._1)
          .transformWithState(
            new EventTimeTimerProcessor(),
            TimeMode.EventTime(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          AddData(inputData, ("a", 1L), ("b", 2L), ("c", 3L)),
          CheckLastBatch(("a", "1"), ("b", "1"), ("c", "1")),
          AddData(inputData, ("a", 4L), ("b", 5L)),
          CheckLastBatch(("a", "2"), ("b", "2")),
          StopStream
        )

        // Verify state data includes event time timer column families
        // 3 keys * (testValueState + $eventTimers_* + $eventTimers_*) = 9 rows
        verifyStateData(
          tempDir.getAbsolutePath,
          expectedRowCount = 18,
          expectedColumnFamilies = Seq(
            "$eventTimers_keyToTimestamp", "$eventTimers_timestampToKey", "testValueState"))
      }
    }
  }

  test("read all column families with stream-stream join v2") {
    withTempDir { tempDir =>
      withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
          classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "2") {

        val inputData1 = MemoryStream[Int]
        val inputData2 = MemoryStream[Int]

        val df1 = inputData1.toDF()
          .select(col("value").as("key"), (col("value") * 2).as("leftValue"))
          .select(col("key"), col("leftValue"), timestamp_seconds(col("key")).as("leftTime"))
          .withWatermark("leftTime", "10 seconds")

        val df2 = inputData2.toDF()
          .select(col("value").as("key"), (col("value") * 3).as("rightValue"))
          .select(col("key"), col("rightValue"), timestamp_seconds(col("key")).as("rightTime"))
          .withWatermark("rightTime", "10 seconds")

        val joined = df1.join(
          df2,
          df1("key") === df2("key"),
          "inner")

        testStream(joined)(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          AddData(inputData1, 1, 2),
          ProcessAllAvailable(),
          AddData(inputData2, 1, 2),
          ProcessAllAvailable(),
          StopStream
        )

        // v2 uses 4 separate state stores (not column families within a single store)
        // StatePartitionReaderAllColumnFamilies reads from all 4 stores
        // and returns store names as column family names
        // 2 keys * 4 stores (left-keyToNumValues, left-keyWithIndexToValue,
        //                     right-keyToNumValues, right-keyWithIndexToValue) = 8 rows
        verifyStateData(tempDir.getAbsolutePath, expectedRowCount = 8,
          expectedColumnFamilies = Seq(
            "left-keyToNumValues",
            "left-keyWithIndexToValue",
            "right-keyToNumValues",
            "right-keyWithIndexToValue"))
      }
    }
  }

  test("read all column families with stream-stream join v3") {
    withTempDir { tempDir =>
      withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
          classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "3") {

        val inputData1 = MemoryStream[Int]
        val inputData2 = MemoryStream[Int]

        val df1 = inputData1.toDF()
          .select(col("value").as("key"), (col("value") * 2).as("leftValue"))
          .select(col("key"), col("leftValue"), timestamp_seconds(col("key")).as("leftTime"))
          .withWatermark("leftTime", "10 seconds")

        val df2 = inputData2.toDF()
          .select(col("value").as("key"), (col("value") * 3).as("rightValue"))
          .select(col("key"), col("rightValue"), timestamp_seconds(col("key")).as("rightTime"))
          .withWatermark("rightTime", "10 seconds")

        val joined = df1.join(
          df2,
          df1("key") === df2("key"),
          "inner")

        testStream(joined)(
          StartStream(checkpointLocation = tempDir.getAbsolutePath),
          AddData(inputData1, 1, 2),
          ProcessAllAvailable(),
          AddData(inputData2, 1, 2),
          ProcessAllAvailable(),
          StopStream
        )

        // v3 uses 1 state store with 4 column families
        // 2 keys * 4 column families = 8 rows
        verifyStateData(tempDir.getAbsolutePath, expectedRowCount = 8,
          expectedColumnFamilies = Seq(
            "left-keyToNumValues",
            "left-keyWithIndexToValue",
            "right-keyToNumValues",
            "right-keyWithIndexToValue"))
      }
    }
  }
}
