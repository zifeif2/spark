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

import java.io.IOException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.v2.state.utils.SchemaUtil
import org.apache.spark.sql.execution.streaming.operators.stateful.join.SymmetricHashJoinStateManager
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.{StateVariableType, TransformWithStateVariableInfo}
import org.apache.spark.sql.execution.streaming.state.{KeyStateEncoderSpec, StateStore, StateStoreColFamilySchema, StateStoreConf, StateStoreId, StateStoreProvider, StateStoreProviderId, StateSchemaProvider}
import org.apache.spark.sql.types.{NullType, StructField, StructType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.SerializableConfiguration

/**
 * An abstract base class for [[DataWriter]] implementations that write data to state store partitions.
 * This class provides common functionality for different state store writer implementations.
 *
 * @param storeConf The state store configuration
 * @param hadoopConf The Hadoop configuration
 * @param partition The state store input partition
 * @param schema The schema of the data to be written
 * @param keyStateEncoderSpec The key state encoder specification
 * @param stateVariableInfoOpt Optional information about state variables
 * @param stateStoreColFamilySchemaOpt Optional column family schema
 * @param stateSchemaProviderOpt Optional state schema provider
 */
abstract class StatePartitionWriterBase(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    partition: StateStoreInputPartition,
    schema: StructType,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema],
    stateSchemaProviderOpt: Option[StateSchemaProvider])
  extends DataWriter[InternalRow] with Logging {

  protected var recordsWritten: Long = 0

  // Used primarily as a placeholder for the value schema in the context of
  // state variables used within the transformWithState operator.
  private val schemaForValueRow: StructType =
    StructType(Array(StructField("__dummy__", NullType)))

  protected val keySchema: StructType = {
    if (SchemaUtil.checkVariableType(stateVariableInfoOpt, StateVariableType.MapState)) {
      SchemaUtil.getCompositeKeySchema(schema, partition.sourceOptions)
    } else {
      SchemaUtil.getSchemaAsDataType(schema, "key").asInstanceOf[StructType]
    }
  }

  protected val valueSchema: StructType = if (stateVariableInfoOpt.isDefined) {
    schemaForValueRow
  } else {
    SchemaUtil.getSchemaAsDataType(schema, "value").asInstanceOf[StructType]
  }

  protected def getStoreUniqueId(
      operatorStateUniqueIds: Option[Array[Array[String]]]): Option[String] = {
    SymmetricHashJoinStateManager.getStateStoreCheckpointId(
      storeName = partition.sourceOptions.storeName,
      partitionId = partition.partition,
      stateStoreCkptIds = operatorStateUniqueIds)
  }

  protected def getStartStoreUniqueId: Option[String] = {
    getStoreUniqueId(partition.sourceOptions.startOperatorStateUniqueIds)
  }

  protected def getEndStoreUniqueId: Option[String] = {
    getStoreUniqueId(partition.sourceOptions.endOperatorStateUniqueIds)
  }

  protected lazy val provider: StateStoreProvider = {
    val stateStoreId = StateStoreId(partition.sourceOptions.stateCheckpointLocation.toString,
      partition.sourceOptions.operatorId, partition.partition, partition.sourceOptions.storeName)
    val stateStoreProviderId = StateStoreProviderId(stateStoreId, partition.queryId)

    val useMultipleValuesPerKey = SchemaUtil.checkVariableType(stateVariableInfoOpt,
      StateVariableType.ListState)

    val provider = StateStoreProvider.createAndInit(
      stateStoreProviderId, keySchema, valueSchema, keyStateEncoderSpec,
      useColumnFamilies = false, storeConf, hadoopConf.value,
      useMultipleValuesPerKey = useMultipleValuesPerKey, stateSchemaProviderOpt)
    provider
  }

  override def commit(): WriterCommitMessage = {
    try {
      commitInternal()
      logInfo(s"Committed state store for partition ${partition.partition}. " +
        s"Records written: $recordsWritten")
      StateWriterCommitMessage(partition.partition, recordsWritten)
    } catch {
      case e: Exception =>
        logError(s"Error committing state store partition ${partition.partition}", e)
        throw new IOException(s"Failed to commit state store: ${e.getMessage}", e)
    }
  }

  override def abort(): Unit = {
    try {
      abortInternal()
      logWarning(s"Aborted state store for partition ${partition.partition}. " +
        s"Records written before abort: $recordsWritten")
    } catch {
      case e: Exception =>
        logError(s"Error aborting state store partition ${partition.partition}", e)
        throw new IOException(s"Failed to abort state store: ${e.getMessage}", e)
    }
  }

  override def close(): Unit = {
    try {
      closeInternal()
      logInfo(s"Closed StatePartitionWriter for partition ${partition.partition}")
    } catch {
      case e: Exception =>
        logError(s"Error closing state store partition ${partition.partition}", e)
    }
  }

  /** Subclasses should implement this to perform the actual commit operation */
  protected def commitInternal(): Unit

  /** Subclasses should implement this to perform the actual abort operation */
  protected def abortInternal(): Unit

  /** Subclasses can override this to perform cleanup. Default is no-op. */
  protected def closeInternal(): Unit = {
    // State store resources are managed by StateStore itself
    // No additional cleanup needed by default
  }
}

/**
 * A [[DataWriter]] implementation that writes data to all column families in a state store partition.
 * This writer is designed to work with data read by [[StatePartitionReaderAllColumnFamilies]],
 * allowing for reading state from all column families and writing it back.
 *
 * @param storeConf The state store configuration
 * @param hadoopConf The Hadoop configuration
 * @param partition The state store input partition
 * @param schema The schema of the data to be written
 * @param keyStateEncoderSpec The key state encoder specification
 * @param stateVariableInfoOpt Optional information about state variables
 * @param stateStoreColFamilySchemaOpt Optional column family schema
 * @param stateSchemaProviderOpt Optional state schema provider
 */
class StatePartitionAllColumnFamiliesWriter(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    partition: StateStoreInputPartition,
    schema: StructType,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema],
    stateSchemaProviderOpt: Option[StateSchemaProvider])
  extends StatePartitionWriterBase(
    storeConf,
    hadoopConf,
    partition,
    schema,
    keyStateEncoderSpec,
    stateVariableInfoOpt,
    stateStoreColFamilySchemaOpt,
    stateSchemaProviderOpt) {

  // Override schemas to use placeholder schemas since we're working with raw bytes
  // The actual schemas are embedded in the bytes themselves in UnsafeRow format
  private val placeholderSchema: StructType =
    StructType(Array(StructField("__dummy__", NullType)))

  override protected val keySchema: StructType = placeholderSchema
  override protected val valueSchema: StructType = placeholderSchema

  // Disable format validation because the schema returned by the reader
  // does not contain the corresponding keySchema or valueSchema.
  // We're working with raw bytes and placeholder schemas.
  private val modifiedStoreConf = storeConf.withExtraOptions(Map(
    StateStoreConf.FORMAT_VALIDATION_ENABLED_CONFIG -> "false",
    StateStoreConf.FORMAT_VALIDATION_CHECK_VALUE_CONFIG -> "false"
  ))

  // Override provider to enable column families support
  override protected lazy val provider: StateStoreProvider = {
    val stateStoreId = StateStoreId(partition.sourceOptions.stateCheckpointLocation.toString,
      partition.sourceOptions.operatorId, partition.partition, partition.sourceOptions.storeName)
    val stateStoreProviderId = StateStoreProviderId(stateStoreId, partition.queryId)

    val useMultipleValuesPerKey = SchemaUtil.checkVariableType(stateVariableInfoOpt,
      StateVariableType.ListState)

    val provider = StateStoreProvider.createAndInit(
      stateStoreProviderId, keySchema, valueSchema, keyStateEncoderSpec,
      useColumnFamilies = true, // Enable column families support
      modifiedStoreConf, hadoopConf.value,
      useMultipleValuesPerKey = useMultipleValuesPerKey, stateSchemaProviderOpt)
    provider
  }

  private lazy val stateStore: StateStore = {
    provider.getStore(partition.sourceOptions.batchId + 1, forceSnapshotOnCommit = true)
  }

  /**
   * Extracts the number of fields from UnsafeRow bytes.
   * UnsafeRow format stores the field count as a little-endian integer in the first 4 bytes.
   */
  private def extractFieldCount(bytes: Array[Byte]): Int = {
    if (bytes.length < 4) {
      throw new IOException(s"Invalid UnsafeRow bytes: length ${bytes.length} < 4")
    }
    // Read little-endian integer from first 4 bytes
    Platform.getInt(bytes, Platform.BYTE_ARRAY_OFFSET)
  }

  override def write(record: InternalRow): Unit = {
    try {
      // Validate record schema
      if (record.numFields != 4) {
        throw new IOException(
          s"Invalid record schema: expected 4 fields (partition_key, key_bytes, value_bytes, " +
          s"column_family_name), got ${record.numFields}")
      }

      // Extract raw bytes and column family name from the record
      val keyBytes = record.getBinary(1)
      val valueBytes = record.getBinary(2)
      val colFamilyName = record.getString(3)

      // Reconstruct UnsafeRow objects from the raw bytes
      // The bytes are in UnsafeRow memory format from StatePartitionReaderAllColumnFamilies
      keyRow.pointTo(keyBytes, Platform.BYTE_ARRAY_OFFSET, keyBytes.length)

      valueRow.pointTo(valueBytes, Platform.BYTE_ARRAY_OFFSET, valueBytes.length)
      
      // Use StateStore API which handles proper RocksDB encoding (version byte, checksums, etc.)
      stateStore.put(keyRow, valueRow, colFamilyName)
      recordsWritten += 1
    } catch {
      case e: Exception =>
        logError(s"Error writing record to state store partition ${partition.partition}", e)
        throw new IOException(s"Failed to write record to state store: ${e.getMessage}", e)
    }
  }

  override protected def commitInternal(): Unit = {
    stateStore.commit()
  }

  override protected def abortInternal(): Unit = {
    stateStore.abort()
  }
}

/**
 * Commit message returned by state partition writers after successful commit.
 *
 * @param partitionId The partition ID
 * @param recordsWritten The number of records written to this partition
 */
case class StateWriterCommitMessage(
    partitionId: Int,
    recordsWritten: Long) extends WriterCommitMessage
