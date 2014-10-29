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
package org.apache.spark.tez

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.ShuffleDependency
import org.apache.spark.Logging
import org.apache.spark.scheduler.Task
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.tez.runtime.library.api.KeyValueWriter
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.TaskContext
import org.apache.spark.Partitioner

/**
 * Tez vertex Task modeled after Spark's ShufleMapTask
 */
class VertexShuffleTask(
  stageId: Int,
  rdd: RDD[_],
  val dep: Option[ShuffleDependency[Any, Any, Any]],
  partitions: Array[Partition]) extends TezTask[MapStatus](stageId, 0, rdd) {

  /*
   * NOTE: While we are not really dependent on the Partition we need it to be non null to 
   * comply with Spark (see ShuffleRDD)
   */

  /**
   * 
   */
  override def runTask(context: TaskContext): MapStatus = {
    val manager = SparkEnv.get.shuffleManager
    val handle = new BaseShuffleHandle(0, 0, dep.get)
    var writer: ShuffleWriter[Any, Any] = manager.getWriter(handle, 0, context)
    
    try {
      val partition = if (partitions.length == 1) partitions(0) else partitions(context.partitionId)
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      return writer.stop(success = true).get
    } finally {
      writer.stop(success = false)
    }
  }
}