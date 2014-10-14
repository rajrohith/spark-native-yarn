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

/**
 * Utility class used as a gateway to DAGBuilder and DAGTask
 */
class Utils[T, U: ClassTag](tezClient:TezClient, stage: Stage, func: (TaskContext, Iterator[T]) => U, localResources:java.util.Map[String, LocalResource]) extends Logging {

  private var vertexId = 0;
  
  private val sparkContext = stage.rdd.context
  
  private val tezConfiguration = new TezConfiguration
  
  val dagBuilder = new DAGBuilder(this.tezClient, this.localResources, tezConfiguration)

  /**
   * 
   */
  def build(keyClass:Class[_ <:Writable], valueClass:Class[_ <:Writable], outputFormatClass:Class[_], outputPath:String):DAGTask = {
    this.prepareDag(stage, null, func, keyClass, valueClass)
    val dagTask = dagBuilder.build(keyClass, valueClass, outputFormatClass, outputPath)
    logInfo("DAG: " + dagBuilder.toString())
    dagTask
  }

  /**
   * 
   */
  private def prepareDag(stage: Stage, dependentStage: Stage, func: (TaskContext, Iterator[T]) => U, keyClass:Class[_], valueClass:Class[_]) {
    if (stage.parents.size > 0) {
      val missing = stage.parents.sortBy(_.id)
      for (parent <- missing) {
        prepareDag(parent, stage, func, keyClass, valueClass)
      }
    }

    val partitioner = ReflectionUtils.getFieldValue(stage.rdd, "partitioner")
    if (partitioner != null && partitioner.isInstanceOf[Option[_]] && partitioner.asInstanceOf[Option[_]].isDefined) {
      val p = partitioner.asInstanceOf[Option[_]].get
      val bos = new ByteArrayOutputStream()
      val os = new TypeAwareObjectOutputStream(bos)
      os.writeObject(p)
      this.dagBuilder.setSparkPartitionerBytes(bos.toByteArray())
    }
      
    val vertexTask =
      if (stage.isShuffleMap) {
        logInfo(stage.shuffleDep.get.toString)
        logInfo("STAGE Shuffle: " + stage + " vertex: " + this.vertexId)
        new VertexShuffleTask(stage.id, stage.rdd, stage.rdd.partitions(0), stage.shuffleDep.asInstanceOf[Option[ShuffleDependency[Any, Any, Any]]])
      } else {
        logInfo("STAGE Result: " + stage + " vertex: " + this.vertexId)
        val dependencies = stage.rdd.getNarrowAncestors.sortBy(_.id)
        new VertexResultTask(stage.id, stage.rdd.asInstanceOf[RDD[T]], stage.rdd.partitions(0), null)
      }
    
    val bos = new ByteArrayOutputStream()
    val os = new TypeAwareObjectOutputStream(bos)
    os.writeObject(vertexTask)
    
    val vertexTaskBuffer = ByteBuffer.wrap(bos.toByteArray())
    
    // will serialize only ParallelCollectionPartition instances. The rest are ignored
    this.serializePartitions(stage.rdd.partitions)

    val dependencies = stage.rdd.getNarrowAncestors.sortBy(_.id)
    val deps = (if (dependencies.size == 0 || dependencies(0).name == null) (for (parent <- stage.parents) yield parent.id).asJavaCollection else dependencies(0))
    val vd = new VertexDescriptor(stage.id, vertexId, deps, vertexTaskBuffer)
    vd.setNumPartitions(stage.numPartitions)
    dagBuilder.addVertex(vd)

    vertexId += 1
  }
  
  /**
   * 
   */
  private def serializePartitions(partitions:Array[Partition]){
//    if (partitions.size > 0 && partitions(0).isInstanceOf[ParallelCollectionPartition[_]]){
//      var partitionCounter = 0
//      for (partition <- partitions) {
//        val partitionPath = new Path(this.sparkContext.appName + "_p_" + partitionCounter)
//        val os = fs.create(partitionPath)
//        logDebug("serializing: " + partitionPath)
//        partitionCounter += 1
//        serializer.serializeStream(os).writeObject(partition).close
//      }
//    }
  }
}
