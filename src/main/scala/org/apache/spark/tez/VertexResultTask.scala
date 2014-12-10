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

import org.apache.spark.Dependency
import org.apache.spark.InterruptibleIterator
import org.apache.spark.OneToOneDependency
import org.apache.spark.Partition
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.rdd.CoGroupPartition
import org.apache.spark.rdd.CoGroupSplitDep
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.ShuffledRDDPartition
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.scheduler.Task
import org.apache.tez.runtime.library.common.ValuesIterator
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.ShuffleDependency
import org.apache.spark.Partitioner
import org.apache.hadoop.io.Writable
import org.apache.spark.tez.io.TezResultWriter
import org.apache.spark.tez.utils.ReflectionUtils
import scala.reflect.ClassTag
import org.apache.hadoop.fs.FileSystem
import org.apache.tez.dag.api.TezConfiguration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.NarrowCoGroupSplitDep

/**
 * Tez vertex Task modeled after Spark's ResultTask
 */
class VertexResultTask[T, U](
  stageId: Int,
  rdd: RDD[T],
  partitions:Array[Partition],
  func: (TaskContext, Iterator[T]) => U = null)
  	extends TezTask[U](stageId, 0, rdd) {
  
  private[tez] var keyClass:Class[Writable] = null
  private[tez] var valueClass:Class[Writable] = null
  
  //TODO review. Need a cleaner way
  private[tez] def setKeyClass(keyClass:Class[Writable]) {
    this.keyClass = keyClass
  }
  
  //TODO review. Need a cleaner way
  private[tez] def setValueClass(valueClass:Class[Writable]) {
    this.valueClass = valueClass
  }

  /**
   *
   */
  override def runTask(context: TaskContext): U = {
    try {
      val partition = if (partitions.length == 1) partitions(0) else partitions(context.partitionId())

      this.resetPartitionIndex(partition, context.partitionId())

      val iterator = if (func == null) {
        rdd.iterator(partition, context).asInstanceOf[Iterator[Product2[_, _]]]
      } else {
        val result = func(context, rdd.iterator(partition, context))
        new InterruptibleIterator(context, Map(partition.index -> result).iterator)
      } 
      this.toHdfs(partition.index, iterator)
    } catch {
      case e: Exception => e.printStackTrace(); throw new IllegalStateException(e)
    }
  }

  /**
   *
   */
  override def toString = "VertexResultTask(" + stageId + ", " + partitionId + ")"
  
  /**
   *
   */
  private def toHdfs(index:Int, iter: Iterator[Product2[Any, Any]]): U = {
    val manager = SparkEnv.get.shuffleManager
    val handle =
      if (rdd.dependencies != Nil && rdd.dependencies.head.isInstanceOf[ShuffleDependency[_, _, _]]) {
        new BaseShuffleHandle(index, 0, rdd.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]])
      } else {
        null
      }
    val writer = manager.getWriter(handle, index, context).asInstanceOf[TezResultWriter[Any, Any, _]]
    writer.setKeyClass(this.keyClass)
    writer.setValueClass(this.valueClass)
    writer.write(iter)
    ().asInstanceOf[U]
  }
}
