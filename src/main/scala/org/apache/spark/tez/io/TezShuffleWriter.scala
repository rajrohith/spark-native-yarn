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
  
  private val EMPTY_STRING = ""
  private val kvOutput = output.values.iterator().next()
  private val kvWriter = kvOutput.getWriter().asInstanceOf[KeyValueWriter]
  private val kw:KeyWritable = new KeyWritable
  private val vw:ValueWritable = new ValueWritable
  /**
   * 
   */
  def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
//    println
//    while (records.hasNext) {
//      val next = records.next.asInstanceOf[Any]
//      println(next)
//    }
    
    
    val (iter, mergeFunction) = {
      val comb = this.buildCombinedIterator(records, combine)
      (comb._1, comb._2)
    }

    this.sinkKeyValuesIterator(iter, mergeFunction)
  }

  /**
   *
   */
  private def sinkKeyValuesIterator(keyValues: Iterator[_ <: Product2[K, V]], mergeFunction: Function2[Any, Any, Any]) {
    var previousKey: Any = null
    var mergedValue: Any = null
    for (keyValue <- keyValues) {    
      this.writeKeyValue(keyValue._1, keyValue._2)
    }
  }

  private def writeKeyValue(key: Any, value: Any) {
    this.kw.setValue(key.asInstanceOf[Comparable[_]])
    this.vw.setValue(value.asInstanceOf[Object])
    kvWriter.write(kw, vw)
  }

  /**
   * 
   */
  def stop(success: Boolean): Option[MapStatus] = {
    Some(SparkUtils.createUnsafeInstance(classOf[CompressedMapStatus]))
  }
  
  /**
   *
   */
  private def buildCombinedIterator(records: Iterator[_ <: Product2[K, V]], combine: Boolean): Tuple2[Iterator[_ <: Product2[K, V]], Function2[Any, Any, Any]] = {
    if (handle != null && handle.dependency.aggregator.isDefined) {
      val aggregator = handle.dependency.aggregator.get

      val mergeValueFunction = aggregator.mergeValue.asInstanceOf[Function2[Any, Any, Any]]
      if (combine) {
        val combiners = new HashMap[Any, Any]
        for (record <- records) {
          if (combiners.contains(record._1)) {
            val v1 = combiners.get(record._1).get
            val mergedValue = mergeValueFunction(v1, record._2)
            combiners.put(record._1, mergedValue)
          } else {
            combiners += record.asInstanceOf[Tuple2[K, V]]
          }
        }
        (combiners.iterator.asInstanceOf[Iterator[_ <: Product2[K, V]]], null)
      } else {
        (records, mergeValueFunction)
      }
    } else {
      (records, null)
    }
  }
}