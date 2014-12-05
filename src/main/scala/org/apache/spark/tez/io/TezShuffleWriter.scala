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

import org.apache.spark.shuffle.ShuffleWriter
import org.apache.hadoop.io.Writable
import org.apache.tez.runtime.api.LogicalOutput
import org.apache.tez.runtime.library.api.KeyValueWriter
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.TaskContext
import scala.reflect.runtime.universe._
import org.apache.spark.SparkEnv
import scala.collection.mutable.HashMap
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.spark.Logging
import org.apache.spark.tez.SparkUtils
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.scheduler.CompressedMapStatus
import org.apache.hadoop.io.NullWritable

/**
 * Implementation of ShuffleWriter which relies on KeyValueWriter provided by Tez
 * to write interim output.
 * Since the output is interim the K/V types are always represented as KeyWritable 
 * and ValueWritable
 */
class TezShuffleWriter[K, V, C](output:java.util.Map[Integer, LogicalOutput], 
    handle: BaseShuffleHandle[K, V, C], 
    context: TaskContext, 
    combine:Boolean = true) extends ShuffleWriter[K, V] with Logging {
  
  private val kvWriter = new MultiTargetKeyValueWriter(output)
  private val kw = new KeyWritable
  private val vw = new ValueWritable[Any]
  private val dep = handle.dependency
  
  KeyWritable.setAscending(dep == null || !dep.keyOrdering.isDefined)
 
  /**
   * 
   */
  override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    this.sinkKeyValuesIterator(records.asInstanceOf[Iterator[Product2[K, V]]])
  }

  /**
   * Will write key/values to shuffle output while handling map-side combine
   */
  private def sinkKeyValuesIterator(keyValues: Iterator[_ <: Product2[K, V]]) {
    val iter =
      if (dep.aggregator.isDefined) {
        if (dep.mapSideCombine) {
          dep.aggregator.get.combineValuesByKey(keyValues, context)
        } else {
          keyValues
        }
      } else if (dep.aggregator.isEmpty && dep.mapSideCombine) {
        throw new IllegalStateException("Aggregator is empty for map-side combine")
      } else {
        keyValues
      }
    
    iter.foreach(keyValue => this.writeKeyValue(keyValue._1, keyValue._2))
  }

  /**
   *
   */
  private def writeKeyValue(key: K, value: Any) {
    this.kw.setValue(key)
    this.vw.setValue(value)
    kvWriter.write(kw, vw)
  }

  /**
   * 
   */
  def stop(success: Boolean): Option[MapStatus] = {
    Some(SparkUtils.createUnsafeInstance(classOf[CompressedMapStatus]))
  }
}