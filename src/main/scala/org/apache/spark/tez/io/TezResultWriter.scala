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
    records.foreach(record => this.write(record._1, record._2))
  }
  /**
   * 
   */
  def stop(success: Boolean): Option[MapStatus] = {
    Some(SparkUtils.createUnsafeInstance(classOf[MapStatus]))
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
  //TODO Highly experimental! Will add support for plugging other Writable conversions via configuration
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
