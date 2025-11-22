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
import java.util.UUID

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.TransformWithStateVariableInfo
import org.apache.spark.sql.execution.streaming.state.{KeyStateEncoderSpec, StateSchemaProvider, StateStore, StateStoreColFamilySchema, StateStoreConf, StateStoreId, StateStoreProviderId}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A [[WriteBuilder]] implementation for State Store data source that provides
 * batch write capabilities to write data to state stores.
 *
 * @param session The Spark session
 * @param info The logical write information including schema and query ID
 * @param schema The schema of the data to be written
 * @param sourceOptions The state source options containing checkpoint location, operator ID, etc.
 * @param stateConf The state store configuration
 * @param keyStateEncoderSpec The key state encoder specification
 * @param stateVariableInfoOpt Optional information about state variables
 * @param stateStoreColFamilySchemaOpt Optional column family schema
 * @param stateSchemaProviderOpt Optional state schema provider
 */
class StateWriteBuilder(
    session: SparkSession,
    info: LogicalWriteInfo,
    schema: StructType,
    sourceOptions: StateSourceOptions,
    stateConf: StateStoreConf,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema],
    stateSchemaProviderOpt: Option[StateSchemaProvider])
  extends WriteBuilder with Logging {

  override def build(): Write = {
    new StateWrite(session, info, schema, sourceOptions, stateConf, keyStateEncoderSpec,
      stateVariableInfoOpt, stateStoreColFamilySchemaOpt, stateSchemaProviderOpt)
  }
}

/**
 * A [[Write]] implementation that returns [[StateBatchWrite]] for batch writes.
 */
private class StateWrite(
    session: SparkSession,
    info: LogicalWriteInfo,
    schema: StructType,
    sourceOptions: StateSourceOptions,
    stateConf: StateStoreConf,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema],
    stateSchemaProviderOpt: Option[StateSchemaProvider])
  extends Write with Logging {

  override def toBatch: BatchWrite = {
    new StateBatchWrite(session, info, schema, sourceOptions, stateConf, keyStateEncoderSpec,
      stateVariableInfoOpt, stateStoreColFamilySchemaOpt, stateSchemaProviderOpt)
  }

  override def description(): String = {
    s"StateWrite[checkpointLocation=${sourceOptions.stateCheckpointLocation}," +
      s"batchId=${sourceOptions.batchId},operatorId=${sourceOptions.operatorId}," +
      s"storeName=${sourceOptions.storeName}]"
  }
}

/**
 * A [[BatchWrite]] implementation for State Store data source that handles
 * the coordination of writing data across multiple partitions.
 */
private class StateBatchWrite(
    session: SparkSession,
    info: LogicalWriteInfo,
    schema: StructType,
    sourceOptions: StateSourceOptions,
    stateConf: StateStoreConf,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema],
    stateSchemaProviderOpt: Option[StateSchemaProvider])
  extends BatchWrite with Logging {

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  private val hadoopConfBroadcast =
    SerializableConfiguration.broadcast(session.sparkContext,
      session.sessionState.newHadoopConf())

  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = {
    new StateDataWriterFactory(
      schema,
      sourceOptions,
      stateConf,
      keyStateEncoderSpec,
      stateVariableInfoOpt,
      stateStoreColFamilySchemaOpt,
      stateSchemaProviderOpt,
      hadoopConfBroadcast)
  }

  override def useCommitCoordinator(): Boolean = {
    // State store writes should use commit coordinator to ensure only one task
    // per partition commits successfully
    true
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logInfo(s"Successfully committed state store write for " +
      s"${sourceOptions.stateCheckpointLocation} at batch ${sourceOptions.batchId}, " +
      s"operator ${sourceOptions.operatorId}, store ${sourceOptions.storeName}. " +
      s"Committed ${messages.length} partitions.")
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logWarning(s"Aborted state store write for " +
      s"${sourceOptions.stateCheckpointLocation} at batch ${sourceOptions.batchId}, " +
      s"operator ${sourceOptions.operatorId}, store ${sourceOptions.storeName}. " +
      s"Aborted ${messages.length} partitions.")
  }
}

/**
 * A [[DataWriterFactory]] that creates [[StatePartitionAllColumnFamiliesWriter]] instances
 * for writing data to state stores.
 */
private class StateDataWriterFactory(
    schema: StructType,
    sourceOptions: StateSourceOptions,
    stateConf: StateStoreConf,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema],
    stateSchemaProviderOpt: Option[StateSchemaProvider],
    hadoopConfBroadcast: SerializableConfiguration)
  extends DataWriterFactory with Logging {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    logInfo(s"Creating StatePartitionAllColumnFamiliesWriter for partition $partitionId, task $taskId")
    
    // Create a StateStoreInputPartition for this writer
    val queryId = UUID.randomUUID()
    val partition = new StateStoreInputPartition(partitionId, queryId, sourceOptions)
    
    new StatePartitionAllColumnFamiliesWriter(
      stateConf,
      hadoopConfBroadcast,
      partition,
      schema,
      keyStateEncoderSpec,
      stateVariableInfoOpt,
      stateStoreColFamilySchemaOpt,
      stateSchemaProviderOpt)
  }
}

