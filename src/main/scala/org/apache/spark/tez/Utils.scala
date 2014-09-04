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

/**
 * Utility class used as a gateway to DAGBuilder and DAGTask
 */
class Utils[T, U: ClassTag](stage: Stage, func: (TaskContext, Iterator[T]) => U) extends Logging {

  private var vertexId = 0;
  
  private val serializer = SparkEnv.get.serializer.newInstance
  
  private val sparkContext = stage.rdd.context
  
  private val tezConfiguration = new TezConfiguration
  
  val fs = FileSystem.get(tezConfiguration);
  val updateClassPath = System.getProperty(TezConstants.UPDATE_CLASSPATH) != null
  if (updateClassPath){
    logInfo("Refreshing application classpath, by deleting the existing one. New one will be provisioned")
    fs.delete(new Path(TezConstants.CLASSPATH_PATH))
  }
  val localResources = YarnUtils.createLocalResources(this.fs, TezConstants.CLASSPATH_PATH)
  
  val tezClient = TezClient.create(sparkContext.appName, tezConfiguration);
  this.tezClient.addAppMasterLocalResources(this.localResources);
  tezClient.start();
  
  val dagBuilder = new DAGBuilder(this.tezClient, this.localResources, tezConfiguration, sparkContext.appName + "_out")
  
  def getConfiguration = tezConfiguration
  /**
   * 
   */
  def build():DAGTask = {
    prepareDag(stage, null, func)
    val dagTask = dagBuilder.build()
    logInfo("DAG: " + dagBuilder.toString())
    dagTask
  }

  /**
   * 
   */
  private def prepareDag(stage: Stage, dependentStage: Stage, func: (TaskContext, Iterator[T]) => U) {
    if (stage.parents.size > 0) {
      val missing = stage.parents.sortBy(_.id)
      for (parent <- missing) {
        prepareDag(parent, stage, func)
      }
    }

    val vertexTask =
      if (stage.isShuffleMap) {
        logInfo(stage.shuffleDep.get.toString)
        logInfo("STAGE Shuffle: " + stage + " - " + stage.rdd.partitions.toList + " vertex: " + vertexId)
        new VertexShuffleTask(stage.id, stage.rdd, stage.shuffleDep.asInstanceOf[Option[ShuffleDependency[Any,Any,Any]]]) 
      } else {
        logInfo("STAGE Result: " + stage + " - " + stage.rdd.partitions.toList + " vertex: " + vertexId)
//        println(stage.rdd.getNarrowAncestors.sortBy(_.id))
//        stage.rdd.a
        val dependencies = stage.rdd.getNarrowAncestors.sortBy(_.id)
        new VertexResultTask(stage.id, stage.rdd.asInstanceOf[RDD[T]], stage.rdd.partitions(0), func) 
      }
    
    val vertexTaskBuffer = serializer.serialize(vertexTask)
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
    if (partitions.size > 0 && partitions(0).isInstanceOf[ParallelCollectionPartition[_]]){
      var partitionCounter = 0
      for (partition <- partitions) {
        val partitionPath = new Path(this.sparkContext.appName + "_p_" + partitionCounter)
        val os = fs.create(partitionPath)
        logDebug("serializing: " + partitionPath)
        partitionCounter += 1
        serializer.serializeStream(os).writeObject(partition).close
      }
    }
  }
}
