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

import org.apache.spark.scheduler.Task
import org.apache.spark.Partitioner
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import org.apache.spark.tez.utils.ReflectionUtils
import org.apache.spark.rdd.CoGroupPartition
import org.apache.spark.rdd.NarrowCoGroupSplitDep
/**
 * 
 */
private[tez] abstract class TezTask[U](stageId: Int, partitionId: Int, val rdd:RDD[_]) extends Task[U](stageId, partitionId) with Logging {

  var partitioner:Partitioner = null
  
  /**
   * 
   */
  private[tez] def setPartitioner(partitioner:Partitioner) {
    this.partitioner = partitioner
  }
  
  /**
   * 
   */
  private[tez] def resetPartitionIndex(partition:Partition, index:Int) {
    var field = ReflectionUtils.findField(partition.getClass(), "index", classOf[Int])
    if (field == null) {
      field = ReflectionUtils.findField(partition.getClass(), "slice", classOf[Int]) // for ParallelCollectionPartition
    }
    if (field != null) {
      field.setAccessible(true)
      field.set(partition, index)
    } else {
      throw new IllegalStateException("Failed to determine index field for " + partition)
    }

    if (partition.isInstanceOf[CoGroupPartition]) {
      partition.asInstanceOf[CoGroupPartition].deps.filter(_.isInstanceOf[NarrowCoGroupSplitDep]).foreach { x =>
        val split = x.asInstanceOf[NarrowCoGroupSplitDep].split
        var idxField = ReflectionUtils.findField(split.getClass(), "index", classOf[Int])
        if (idxField != null) {
          idxField.setAccessible(true)
          idxField.set(split, index)
        } else {
          throw new IllegalStateException("Failed to determine index field for " + partition)
        }
      }
    }
  }
}