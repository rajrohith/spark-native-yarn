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
package org.apache.spark.tez.adapter

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.TaskContext
import org.apache.hadoop.io.Writable
import org.apache.spark.Logging
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.Partitioner
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.tez.io.ValueWritable
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.Aggregator
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.serializer.Serializer
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkException
import org.apache.spark.InterruptibleIterator
import org.apache.spark.rdd.OrderedRDDFunctions
import scala.reflect.ClassTag
import org.apache.spark.RangePartitioner
import scala.math.Ordering

/**
 * Delegating adapter which provides instrumentation overrides for selected OrderedRDDFunctions methods.
 * The actual invocations are delegated to its companion object mainly to simplify debugging.
 */
class OrderedRDDFunctionsAdapter[K : Ordering : ClassTag,
                          V: ClassTag,
                          P <: Product2[K, V] : ClassTag] extends Logging {

  /**
   * 
   */
  def sortByKey(ascending: Boolean = true, numPartitions: Int = 1): RDD[(K, V)] = {
    val ordd = this.asInstanceOf[OrderedRDDFunctions[K, V, P]]
    var fields = ordd.getClass().getDeclaredFields().filter(_.getName().endsWith("self"))
    var field = fields(0)
    field.setAccessible(true)
    val self: RDD[P] = field.get(ordd).asInstanceOf[RDD[P]]
    
    if (ascending){
      new ShuffledRDD[K, V, V](self, new HashPartitioner(numPartitions))
    } else {
      fields = ordd.getClass().getDeclaredFields().filter(_.getName().endsWith("ordering"))
      field = fields(0)
      field.setAccessible(true)
      val ordering = field.get(ordd).asInstanceOf[Ordering[K]]
      new ShuffledRDD[K, V, V](self, new HashPartitioner(numPartitions)).setKeyOrdering(ordering)
    }
  }
}