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
import org.apache.spark.scheduler.CompressedMapStatus
import org.apache.hadoop.io.NullWritable

/**
 * 
 */
class TezResultWriter[K, V, C](output:java.util.Map[Integer, LogicalOutput], handle: BaseShuffleHandle[K, V, C], context: TaskContext) extends ShuffleWriter[K, V] with Logging {
  
  private val kvOutput = output.values.iterator().next()
  private val kvWriter = kvOutput.getWriter().asInstanceOf[KeyValueWriter]
  
  private[tez] var keyWritable:Writable = null
  private[tez] var valueWritable:Writable = null

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
        this.write(record._1, record._2)
      }
    }
    // last element need to be flushed
    if (previousKey != null) {
      this.write(previousKey, mergedValue)
    }
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
  private[tez] def setKeyClass(keyClass:Class[Writable]) {
    this.keyWritable = this.buildWritable(keyClass)
  }
  
  /**
   * 
   */
  private[tez] def setValueClass(valueClass:Class[Writable]) {
    this.valueWritable = this.buildWritable(valueClass)
  }
  
  /**
   * 
   */
  private def write(key:Any, value:Any) {
   if (key.isInstanceOf[Writable] && value.isInstanceOf[Writable]){
     kvWriter.write(key, value)
   }
   else {
     this.keyWritable.asInstanceOf[NewWritable[Any]].setValue(key)
     this.valueWritable.asInstanceOf[NewWritable[Any]].setValue(value)
     kvWriter.write(this.keyWritable, this.valueWritable)
   }
  }
  
  /**
   * 
   */
  private def buildWritable(wClass:Class[Writable]):Writable = {
    if (wClass.isAssignableFrom(classOf[IntWritable])){
      new NewWritable.NewIntWritable
    } else if (wClass.isAssignableFrom(classOf[LongWritable])){
      new NewWritable.NewLongWritable
    } else if (wClass.isAssignableFrom(classOf[Text])){
      new NewWritable.NewTextWritable
    } else if (wClass.isAssignableFrom(classOf[KeyWritable])){
      new KeyWritable
    } else if (wClass.isAssignableFrom(classOf[ValueWritable])){
      new ValueWritable
    } else if (wClass.isAssignableFrom(classOf[NullWritable])){
      NullWritable.get
    } else {
      throw new IllegalStateException("Unrecognized writable: " + wClass)
    }
  }
}
