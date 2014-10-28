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
package org.apache.spark.tez.io

import java.lang.Iterable
import java.lang.reflect.Field
import java.nio.ByteBuffer
import java.util.Map
import scala.collection.Iterator
import scala.collection.JavaConverters._
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.WritableComparator
import org.apache.spark.Logging
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.tez.runtime.api.LogicalInput
import org.apache.tez.runtime.api.LogicalOutput
import org.apache.tez.runtime.api.Reader
import org.apache.tez.runtime.library.api.KeyValueReader
import org.apache.tez.runtime.library.api.KeyValueWriter
import org.apache.tez.runtime.library.api.KeyValuesReader
import org.apache.hadoop.io.IntWritable
import scala.reflect.runtime.universe._

/**
 * Implementation of Spark's ShuffleManager to support Spark's task connectivity 
 * to Tez's readers and writers
 * 
 */
class TezShuffleManager(val input:Map[Integer, LogicalInput], val output:Map[Integer, LogicalOutput]) extends ShuffleManager with Logging{
  logDebug("Creating TezMapShuffleManager")
  
  private[tez] var shuffleStage = true
  
  /**
   * 
   */
  def registerShuffle[K, V, C](
    shuffleId: Int,
    numMaps: Int,
    dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    null
  }

  /** 
   *  Get a writer for a given partition. Called on executors by map tasks. 
   */
  def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V] = {
    if (shuffleStage){
      new TezShuffleWriter(this.output, handle.asInstanceOf[BaseShuffleHandle[K, V, _]], context, shuffleStage)
    }
    else {
      new TezResultWriter(this.output,  handle.asInstanceOf[BaseShuffleHandle[K, V, _]], context)
    }
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  def getReader[K, C](handle: ShuffleHandle, startPartition: Int, endPartition: Int, context: TaskContext): ShuffleReader[K, C] = {
    if (shuffleStage){
      new TezSourceReader(this.input)
    }
    else {
      new TezShuffleReader(this.input, handle.asInstanceOf[BaseShuffleHandle[K, C, _]])
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  def unregisterShuffle(shuffleId: Int){}


  /** Shut down this ShuffleManager. */
  def stop(): Unit = ()
} 