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

/**
 * 
 */
class TezResultWriter[K, V, C](output:java.util.Map[Integer, LogicalOutput], handle: BaseShuffleHandle[K, V, C]) extends ShuffleWriter[K, V] with Logging {
  
  private val kvOutput = output.values.iterator().next()
  private val kvWriter = kvOutput.getWriter().asInstanceOf[KeyValueWriter]

  /**
   *
   */
  def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    val mergeValueFunction: Function2[Any, Any, Any] =
      if (handle != null && handle.dependency.aggregator.isDefined) {
        val aggregator = handle.dependency.aggregator.get
        aggregator.mergeValue.asInstanceOf[Function2[Any, Any, Any]]
      } else {
        null
      }

    this.sinkKeyValuesIterator(records, mergeValueFunction)
  }
  
  /**
   * 
   */
  /*
   * TODO
   * For cases such as count or other the iterator may not be KV. It may just be a single value
   * so we need somethimg lieke this:
   * for (value <- iterator) {
        println(value)
        if (value.isInstanceOf[Product2[K, V]]){
          println("product")
        }
        else {
          println("other")
        }
        
        kvWriter.write(NullWritable.get(), value)
      }
   */
  private def sinkKeyValuesIterator(records: Iterator[_ <: Product2[K, V]], mergeFunction:Function2[Any,Any,Any]) {
    var previousKey:Any = null
    var mergedValue: Any = null
    for (record <- records) {
      if (mergeFunction != null) {
        if (previousKey == null) {     
          previousKey = record._1
          mergedValue = record._2
        } else if (previousKey == record._1) {
          mergedValue = mergeFunction(mergedValue, record._2)
        } else {
          kvWriter.write(previousKey, mergedValue)
          previousKey = record._1
          mergedValue = record._2
        }
      } else {
         kvWriter.write(record._1, record._2)
      }
    }
    // last element need to be flushed
    if (previousKey != null) {
      kvWriter.write(previousKey, mergedValue)
    }
  }

  /**
   * 
   */
  def stop(success: Boolean): Option[MapStatus] = {
    Some(SparkUtils.createUnsafeInstance(classOf[MapStatus]))
  }
}