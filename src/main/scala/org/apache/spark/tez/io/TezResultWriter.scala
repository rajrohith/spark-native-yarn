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
class TezResultWriter[K, V, C](output:java.util.Map[Integer, LogicalOutput], 
    handle: BaseShuffleHandle[K, V, C], 
    context: TaskContext, 
    combine:Boolean = true) extends ShuffleWriter[K, V] with Logging {
  
  private val kvOutput = output.values.iterator().next()
  private val kvWriter = kvOutput.getWriter().asInstanceOf[KeyValueWriter]
  private val serializer = SparkEnv.get.serializer.newInstance
  private var kw:Writable = null
  private var vw:Writable = null
  /**
   * 
   */
  def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    this.sinkKeyValuesIterator(records, null)
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
  private def sinkKeyValuesIterator(keyValues: Iterator[_ <: Product2[K, V]], mergeFunction:Function2[Any,Any,Any]) {
    var previousKey:Any = null
    var mergedValue: Any = null
    for (keyValue <- keyValues) {
      if (mergeFunction != null) {
        if (previousKey == null) {     
          previousKey = keyValue._1
          mergedValue = keyValue._2
        } else if (previousKey == keyValue._1) {
          mergedValue = mergeFunction(mergedValue, keyValue._2)
        } else {
          this.writeKeyValue(previousKey, mergedValue)
          previousKey = keyValue._1
          mergedValue = keyValue._2
        }
      } else {
        this.writeKeyValue(keyValue._1, keyValue._2)
      }
    }
    // last element need to be flushed
    if (previousKey != null) {
      this.writeKeyValue(previousKey, mergedValue)
    }
  }

  private def writeKeyValue(key: Any, value: Any) {
    kvWriter.write(key, value)
  }

  /**
   * 
   */
  def stop(success: Boolean): Option[MapStatus] = {
    Some(SparkUtils.createUnsafeInstance(classOf[MapStatus]))
  }

  /*
   * Below code (two methods) is temporary and will be exposed thru TypeConversion and Serialization framework 
   */
  private def toKeyWritable(value: Any) = {
    if (kw == null) {
      kw =
        if (value.isInstanceOf[Integer]) {
          new IntWritable(value.asInstanceOf[Integer])
        } else if (value.isInstanceOf[Long]) {
          new LongWritable(value.asInstanceOf[Long])
        } else if (value.isInstanceOf[String]) {
          new Text(value.toString)
        } else {
          throw new IllegalStateException("Unsupported type: " + value.getClass)
        }
    } 
    else {
      if (kw.isInstanceOf[Text]){
        kw.asInstanceOf[Text].set(value.toString)
      } 
      else if (kw.isInstanceOf[IntWritable]) {
        kw.asInstanceOf[IntWritable].set(value.asInstanceOf[Integer])
      }
      else if (kw.isInstanceOf[LongWritable]) {
        kw.asInstanceOf[LongWritable].set(value.asInstanceOf[Long])
      }
    }
  }
  
  private def toValueWritable(value: Any) = {
    if (vw == null) {
      vw =
        if (value.isInstanceOf[Integer]) {
          new IntWritable(value.asInstanceOf[Integer])
        } else if (value.isInstanceOf[Long]) {
          new LongWritable(value.asInstanceOf[Long])
        } else if (value.isInstanceOf[String]) {
          new Text(value.toString)
        } else {
          throw new IllegalStateException("Unsupported type: " + value.getClass)
        }
    } 
    else {
      if (vw.isInstanceOf[Text]){
        vw.asInstanceOf[Text].set(value.toString)
      } 
      else if (vw.isInstanceOf[IntWritable]) {
        vw.asInstanceOf[IntWritable].set(value.asInstanceOf[Integer])
      }
      else if (vw.isInstanceOf[LongWritable]) {
        vw.asInstanceOf[LongWritable].set(value.asInstanceOf[Long])
      }
    }
  }
}