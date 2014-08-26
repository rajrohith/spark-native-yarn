//package org.apache.spark.tez
//
//import scala.collection.JavaConverters.asJavaCollectionConverter
//import scala.collection.mutable.HashMap
//import scala.collection.mutable.HashSet
//import scala.collection.mutable.Stack
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.FileSystem
//import org.apache.hadoop.fs.Path
//import org.apache.spark.NarrowDependency
//import org.apache.spark.Partition
//import org.apache.spark.ShuffleDependency
//import org.apache.spark.SparkEnv
//import org.apache.spark.rdd.ParallelCollectionPartition
//import org.apache.spark.rdd.ParallelCollectionRDD
//import org.apache.spark.rdd.RDD
//import org.apache.spark.scheduler.Stage
//import org.apache.spark.serializer.SerializerInstance
//import org.apache.spark.util.CallSite
//import org.apache.tez.client.TezClient
//import org.apache.tez.dag.api.TezConfiguration
//
//import com.hortonworks.spark.tez.utils.YarnUtils
//
//
///**
// * 
// */
//class DAGUtils(applicationName:String, tezConfiguration:Configuration, func:Function2[_,_,_]) {
//
//  private val shuffleToMapStage = new HashMap[Int, Stage]
////  private val tezConfiguration = new TezConfiguration(sc.hadoopConfiguration)
//  
//  val fs = FileSystem.get(this.tezConfiguration);
//  val localResources = YarnUtils.createLocalResources(this.fs, "stark-cp")
//  
//  val tezClient = new TezClient(this.applicationName, this.tezConfiguration.asInstanceOf[TezConfiguration]);
//  this.tezClient.addAppMasterLocalResources(this.localResources);
//  tezClient.start();
// 
////  private val dagBuilder = new DAGBuilder(this.tezClient, this.localResources, this.tezConfiguration, this.applicationName + "_out")
//  private val dagBuilder:DAGBuilder = null
//  
//  private var stageIdCounter = 0;
//  private var vertexId = 0;
//  
//  def buildDAGTask(rdd: RDD[_],
//    callSite: CallSite):DAGTask = {
//    
////    rdd.getNarrowAncestors
//    
//    
//    this.processStage(rdd, 1, None, 1, callSite)
////    val dagTask = dagBuilder.build()
////    dagTask
//    null
//  }
//
//  private def normalizeForTez(rdd: RDD[_]): Unit = {
//    //    val ancestors = new mutable.HashSet[RDD[_]]
//
//    def visit(rdd: RDD[_]) {
//      println(rdd.dependencies)
//      if (rdd.dependencies(0).rdd.isInstanceOf[TezRDD[_, _]] || rdd.dependencies(0).rdd.isInstanceOf[ParallelCollectionRDD[_]]) {
//        val f = classOf[RDD[_]].getDeclaredField("deps")
//        f.setAccessible(true)
//        f.set(rdd, Nil)
//      } else {
//        val narrowDependencies = rdd.dependencies.filter(_.isInstanceOf[NarrowDependency[_]])
//        val narrowParents = narrowDependencies.map(_.rdd)
//        narrowParents.foreach { parent =>
//          visit(parent)
//        }
//      }
//    }
//    visit(rdd)
//  }
//  
//  
//  override def toString():String = {
//    dagBuilder.toString
//  }
//  
//  /**
//   * 
//   */
//  private def processStage(
//    rdd: RDD[_],
//    numTasks: Int,
//    shuffleDep: Option[ShuffleDependency[_, _, _]],
//    jobId: Int,
//    callSite: CallSite): Stage = {
//    
//    val serializer = SparkEnv.get.serializer.newInstance
//    
//    val id = stageIdCounter
//    stageIdCounter += 1
//    
////    rdd.getNarrowAncestors.sortBy(f)
//    val stage = new Stage(id, rdd, numTasks, shuffleDep, getParentStages(rdd, jobId), jobId, callSite)
//    this.normalizeForTez(rdd)
//    
//    val vertexTask =
//      if (stage.isShuffleMap) {
//        new VertexTask(stage.rdd)
//        println("STAGE Shuffle: " + stage + " - " + stage.rdd.partitions.toList + " vertex: " + vertexId)
//      } else {
//        new VertexResultTask(stage.rdd, func)
//        println("STAGE Result: " + stage + " - " + stage.rdd.partitions.toList + " vertex: " + vertexId)
//      }
//    
//    val taskSerBuffer = serializer.serialize(vertexTask)
//    // will serialize only ParallelCollectionPartition instances. The rest are ignored
//    this.serializePartitions(stage.rdd.partitions, serializer)
//    
//    
//    val dependencies = stage.rdd.getNarrowAncestors.sortBy(_.id)
////    val firstDependency = dependencies(0)
//    val deps = (if (dependencies.size == 0) (for (parent <- stage.parents) yield parent.id).asJavaCollection else if (dependencies(0).name == null) Nil.asJavaCollection else dependencies(0).name)
//    
//    
//    val vd = new VertexDescriptor(id, vertexId, deps, taskSerBuffer)
//    println(vd)
//    vd.setNumPartitions(stage.numPartitions)
////    dagBuilder.addVertex(vd)
//      
//    vertexId += 1
//    stage
//  }
//  
//  /**
//   * 
//   */
//  private def serializeRdd(stage: Stage, serializer:SerializerInstance)  = {
//    val byteBuffer = serializer.serialize(stage.rdd)
//    
//  }
//  
//  /**
//   * 
//   */
//  private def serializePartitions(partitions:Array[Partition], serializer:SerializerInstance){
//    if (partitions.size > 0 && partitions(0).isInstanceOf[ParallelCollectionPartition[_]]){
//      var partitionCounter = 0
//      for (partition <- partitions) {
//        val partitionPath = new Path(this.applicationName + "_p_" + partitionCounter)
//        val os = fs.create(partitionPath)
//        println("serializing: " + partitionPath)
//        partitionCounter += 1
//        serializer.serializeStream(os).writeObject(partition).close
//      }
//    }
//  }
//
//  private def getParentStages(rdd: RDD[_], jobId: Int): List[Stage] = {
//    val parents = new HashSet[Stage]
//    val visited = new HashSet[RDD[_]]
//    // We are manually maintaining a stack here to prevent StackOverflowError
//    // caused by recursively visiting
//    val waitingForVisit = new Stack[RDD[_]]
//    def visit(r: RDD[_]) {
//      if (!visited(r)) {
//        visited += r
//        // Kind of ugly: need to register RDDs with the cache here since
//        // we can't do it in its constructor because # of partitions is unknown
//        for (dep <- r.dependencies) {
//          dep match {
//            case shufDep: ShuffleDependency[_, _, _] =>
//              parents += getShuffleMapStage(shufDep, jobId)
//            case _ =>
//              waitingForVisit.push(dep.rdd)
//          }
//        }
//      }
//    }
//    waitingForVisit.push(rdd)
//    while (!waitingForVisit.isEmpty) {
//      visit(waitingForVisit.pop())
//    }
//    parents.toList
//  }
//
//  private def getShuffleMapStage(shuffleDep: ShuffleDependency[_, _, _], jobId: Int): Stage = {
//    shuffleToMapStage.get(shuffleDep.shuffleId) match {
//      case Some(stage) => stage
//      case None =>
//        // We are going to register ancestor shuffle dependencies
//        registerShuffleDependencies(shuffleDep, jobId)
//        // Then register current shuffleDep
//        val stage =
//          newOrUsedStage(
//            shuffleDep.rdd, shuffleDep.rdd.partitions.size, shuffleDep, jobId,
//            shuffleDep.rdd.creationSite)
//        shuffleToMapStage(shuffleDep.shuffleId) = stage
//
//        stage
//    }
//  }
//
//  private def registerShuffleDependencies(shuffleDep: ShuffleDependency[_, _, _], jobId: Int) = {
//    val parentsWithNoMapStage = getAncestorShuffleDependencies(shuffleDep.rdd)
//    while (!parentsWithNoMapStage.isEmpty) {
//      val currentShufDep = parentsWithNoMapStage.pop()
//      val stage = newOrUsedStage(
//          currentShufDep.rdd, currentShufDep.rdd.partitions.size, currentShufDep, jobId,
//          currentShufDep.rdd.creationSite)
//      shuffleToMapStage(currentShufDep.shuffleId) = stage
//    }
//  }
//
//  private def getAncestorShuffleDependencies(rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {
//    val parents = new Stack[ShuffleDependency[_, _, _]]
//    val visited = new HashSet[RDD[_]]
//    // We are manually maintaining a stack here to prevent StackOverflowError
//    // caused by recursively visiting
//    val waitingForVisit = new Stack[RDD[_]]
//    def visit(r: RDD[_]) {
//      if (!visited(r)) {
//        visited += r
//        for (dep <- r.dependencies) {
//          dep match {
//            case shufDep: ShuffleDependency[_, _, _] =>
//              if (!shuffleToMapStage.contains(shufDep.shuffleId)) {
//                parents.push(shufDep)
//              }
//              waitingForVisit.push(shufDep.rdd)
//            case _ =>
//              waitingForVisit.push(dep.rdd)
//          }
//        }
//      }
//    }
//
//    waitingForVisit.push(rdd)
//    while (!waitingForVisit.isEmpty) {
//      visit(waitingForVisit.pop())
//    }
//    parents
//  }
//
//  private def newOrUsedStage(
//    rdd: RDD[_],
//    numTasks: Int,
//    shuffleDep: ShuffleDependency[_, _, _],
//    jobId: Int,
//    callSite: CallSite): Stage = {
//    val stage = processStage(rdd, numTasks, Some(shuffleDep), jobId, callSite)
//    stage
//  }
//
//}