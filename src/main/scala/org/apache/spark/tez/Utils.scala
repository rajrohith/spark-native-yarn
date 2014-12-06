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

import scala.collection.JavaConverters.asJavaCollectionConverter
import scala.reflect.ClassTag
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.Partition
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.rdd.ParallelCollectionPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.Stage
import org.apache.tez.client.TezClient
import org.apache.tez.dag.api.TezConfiguration
import org.apache.spark.tez.io.TypeAwareStreams.TypeAwareObjectOutputStream
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.lang.Boolean
import org.apache.spark.tez.io.TezRDD
import java.io.FileNotFoundException
import org.apache.hadoop.io.Writable
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.tez.utils.ReflectionUtils
import org.apache.spark.Partitioner
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.spark.rdd.ParallelCollectionRDD
import java.util.HashMap
import scala.collection.JavaConverters._
import org.apache.spark.tez.io.TezRDD
import org.apache.hadoop.io.NullWritable
import org.apache.spark.storage.StorageLevel
import java.util.Collection
import org.apache.spark.rdd.CoGroupedRDD
import scala.collection.mutable.ListBuffer

/**
 * Utility class used as a gateway to DAGBuilder and DAGTask
 */
class Utils[T, U: ClassTag](stage: Stage, func: (TaskContext, Iterator[T]) => U, 
    localResources:java.util.Map[String, LocalResource] = new java.util.HashMap[String, LocalResource]) extends Logging {

  private var vertexId = 0;
  
  private val sparkContext = stage.rdd.context
  
  private val tezConfiguration = new TezConfiguration
  
  private val dagBuilder = new DAGBuilder(stage.rdd.context.appName, this.localResources, tezConfiguration)
  
  private val fs = FileSystem.get(tezConfiguration)

  /**
   * 
   */
  def build(returnType:ClassTag[_], keyClass:Class[_ <:Writable], valueClass:Class[_ <:Writable], outputFormatClass:Class[_], outputPath:String):DAGTask = {
    this.prepareDag(returnType, stage, null, func, keyClass, valueClass)
    val dagTask = dagBuilder.build(keyClass, valueClass, outputFormatClass, outputPath)
    logInfo("DAG: " + dagBuilder)
    dagTask
  }

  /**
   * 
   */
  private def prepareDag(returnType:ClassTag[_], stage: Stage, dependentStage: Stage, func: (TaskContext, Iterator[T]) => U, keyClass:Class[_], valueClass:Class[_]) {
    val mayHaveMissingStages = if (stage.rdd.getStorageLevel != StorageLevel.NONE) {
      val appName = stage.rdd.context.appName
      val id = stage.rdd.id
      val cacheDir = appName + "/cache/cache_" + stage.rdd.id
      if (fs.exists(new Path(cacheDir))) {
        logDebug("RDD " + stage.rdd + " is cached in " + cacheDir)
        false
      } else {
        logDebug("RDD " + stage.rdd + " is not cached")
        true
      }
    } else {
      true
    }
    
    if (mayHaveMissingStages && stage.parents.size > 0) {
      stage.parents.filter(ps => !this.dagBuilder.containsVertexDescriptor(ps.id)).sortBy(_.id).
      	foreach{prepareDag(returnType, _, stage, func, keyClass, valueClass)}
    }

    this.addPartitionerToDAGIfAvailable(stage.rdd)
    
    val partitions = {
      val narrowAncestors = stage.rdd.getNarrowAncestors.sortBy(_.id)
      if (narrowAncestors.size > 0 && narrowAncestors.head.isInstanceOf[ParallelCollectionRDD[_]]) {
        stage.rdd.partitions
      } else {
        Array(stage.rdd.partitions(0))
      }
    }

    val vertexTask =
      if (stage.isShuffleMap) {
        logInfo("STAGE Shuffle: " + stage + " vertex: " + this.vertexId)
        new VertexShuffleTask(stage.id, stage.rdd, stage.shuffleDep.asInstanceOf[Option[ShuffleDependency[Any, Any, Any]]], partitions)
      } else {
        logInfo("STAGE Result: " + stage + " vertex: " + this.vertexId)
        if (returnType == null || classOf[Unit].isAssignableFrom(returnType.runtimeClass)) {
          new VertexResultTask(stage.id, stage.rdd.asInstanceOf[RDD[T]], partitions)
        } else {
          new VertexResultTask(stage.id, stage.rdd.asInstanceOf[RDD[T]], partitions, func)
        }
      }

    var dependencies = stage.rdd.getNarrowAncestors.sortBy(_.id)

    var deps = if (!mayHaveMissingStages) {
      null
    } else {
        if (dependencies.size == 0 || dependencies(0).name == null) 
          (for (parent <- stage.parents) yield parent.id).asJavaCollection else dependencies(0)
    }
    
    val vd = new VertexDescriptor(stage.id, vertexId, deps, vertexTask)

    vd.setNumPartitions(stage.numPartitions)
    dagBuilder.addVertex(vd)

    vertexId += 1
  }
    
  /**
   * 
   */
  private def addPartitionerToDAGIfAvailable(rdd:RDD[_]) {
    val partitioner = ReflectionUtils.getFieldValue(rdd, "partitioner").asInstanceOf[Option[Partitioner]]
    if (partitioner.isDefined) this.dagBuilder.addPartitioner(partitioner.get)
  }
}
