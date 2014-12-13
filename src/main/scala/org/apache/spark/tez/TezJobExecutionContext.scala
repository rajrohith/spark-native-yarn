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

import org.apache.spark.JobExecutionContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{ FileInputFormat, InputFormat, JobConf }
import org.apache.hadoop.mapreduce.{ InputFormat => NewInputFormat, Job => NewHadoopJob }
import org.apache.hadoop.mapreduce.lib.input.{ FileInputFormat => NewFileInputFormat }
import org.apache.hadoop.mapreduce.lib.output.{ FileOutputFormat => NewFileOutputFormat }
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.TaskContext
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.scheduler.Stage
import org.apache.spark.ShuffleDependency
import org.apache.spark.util.CallSite
import org.apache.spark.SparkHadoopWriter
import org.apache.spark.SerializableWritable
import org.apache.spark.Logging
import org.apache.spark.tez.io.TezRDD
import org.apache.spark.tez.adapter.SparkToTezAdapter
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.tez.io.ValueWritable
import org.apache.spark.tez.io.ValueWritable
import org.apache.spark.tez.io.KeyWritable
import org.apache.tez.dag.api.TezConfiguration
import java.io.BufferedReader
import java.io.InputStreamReader
import java.lang.Long
import java.lang.Exception
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.HashSet
import java.io.FileNotFoundException
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.tez.client.TezClient
import java.util.concurrent.atomic.AtomicInteger
import java.util.ArrayList
import java.util.Arrays
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.Partition
import org.apache.hadoop.io.SequenceFile
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.yarn.conf.YarnConfiguration
import scala.reflect.runtime.universe._
import scala.reflect.ManifestFactory
import org.apache.spark.tez.utils.ReflectionUtils
import scala.collection.mutable.HashMap
import org.apache.spark.scheduler.TaskLocation
import scala.collection.mutable.Stack
import org.apache.spark.NarrowDependency
import org.apache.spark.rdd.CoGroupedRDD
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkContext._
import org.apache.spark.OneToOneDependency
import org.apache.spark.util.collection.CompactBuffer

/**
 *
 */
class TezJobExecutionContext extends JobExecutionContext with Logging {
  SparkToTezAdapter.adapt
  
  val hadoopConf = System.getenv().get("HADOOP_CONF_DIR")
  logInfo("Config dir: " + hadoopConf)
  try {
    logInfo("FileSystem: " + new TezConfiguration().get("fs.defaultFS"))
  } catch {
    case e:Exception => {
      val confLoadLocation = classOf[Configuration].getProtectionDomain().getCodeSource().getLocation()
      val tezConfLoadLocation = classOf[TezConfiguration].getProtectionDomain().getCodeSource().getLocation()
      logError("Error while accessing configuration. Possible cause - 'version missmatch'\n" + 
    		 "org.apache.hadoop.conf.Configuration is loaded from " + confLoadLocation + "\n" + 
    		 "org.apache.tez.dag.api.TezConfiguration is loaded from " + tezConfLoadLocation)
      e.printStackTrace()
    }
  }
  
  
  
  private val cachedRDDs = new HashSet[Path]

  private val runJobInvocationCounter = new AtomicInteger

  private val tezDelegate = new TezDelegate

  private val fs = FileSystem.get(tezDelegate.tezConfiguration)
  
  private val cacheLocs = new HashMap[Int, Array[Seq[TaskLocation]]]
  
  private val jobIds = new AtomicInteger

  /**
   *
   */
  def hadoopFile[K, V](
    sc: SparkContext,
    path: String,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int = 1): RDD[(K, V)] = {

    new TezRDD(path, sc, inputFormatClass, keyClass, valueClass, sc.hadoopConfiguration)
  }

  /**
   *
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
    sc: SparkContext,
    path: String,
    fClass: Class[F],
    kClass: Class[K],
    vClass: Class[V],
    conf: Configuration = new Configuration): RDD[(K, V)] = {

    new TezRDD(path, sc, fClass, kClass, vClass, conf)
  }

  /**
   *
   */
  def broadcast[T: ClassTag](sc: SparkContext, value: T): Broadcast[T] = {
    val tezBroadcast = new TezBroadcast(value, sc.appName)
    tezBroadcast.broadcast
    tezBroadcast
  }

  /**
   *
   */
  def runJob[T, U](
    sc: SparkContext,
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    allowLocal: Boolean,
    resultHandler: (Int, U) => Unit)(implicit returnType: ClassTag[U]) = {

    logDebug("RETURN TYPE for Job: " + returnType)
    
    if (this.runJobInvocationCounter.getAndIncrement() == 0){
      sc.addSparkListener(this.tezDelegate)
    }

    val operationName = SparkUtils.getLastMethodName
    val finalStage = this.caclulateStages(rdd, jobIds.getAndIncrement())

    val outputPath = this.tezDelegate.submitApplication[T, U](returnType, finalStage, func)
    val shuffleToMapStage = ReflectionUtils.getFieldValue(sc.dagScheduler, "shuffleToMapStage").asInstanceOf[HashMap[_,_]]
    shuffleToMapStage.clear

    this.processResult(sc, outputPath, partitions.size, operationName, returnType, resultHandler)
  }

  /**
   *
   */
  private def processResult[U](sc: SparkContext, out:String, partitionSize: Int, operationName: String, returnType: ClassTag[U], resultHandler: (Int, U) => Unit) {
    if (!classOf[Unit].isAssignableFrom(returnType.runtimeClass)) {
      logDebug("Building non Unit result")
      val conf = new TezConfiguration
      val outputPath = new Path(out)
      val iter = fs.listFiles(outputPath, false)
      
      var partitionCounter = 0
      while (iter.hasNext() && partitionCounter < partitionSize) {
        val fStatus = iter.next()
        if (!fStatus.getPath().toString().endsWith("_SUCCESS")) {
          val reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(fStatus.getPath()));
          val kw = reader.getKeyClass().newInstance().asInstanceOf[org.apache.spark.tez.io.NewWritable[_]]
          val vw = reader.getValueClass().newInstance().asInstanceOf[org.apache.spark.tez.io.NewWritable[_]]
          reader.next(kw, vw)
          val value = vw.getValue
          resultHandler(partitionCounter, value.asInstanceOf[U])
          reader.close()
          partitionCounter += 1
        }
      }
      fs.delete(outputPath, true)
      logDebug("Deleted job's output path: " + outputPath)
    }
  }

  /**
   *
   */
  private def caclulateStages(rdd: RDD[_], jobId:Int): Stage = {
    val ds = rdd.context.dagScheduler
    val method = ds.getClass.getDeclaredMethod("newStage", classOf[RDD[_]], classOf[Int], classOf[Option[ShuffleDependency[_, _, _]]], classOf[Int], classOf[CallSite])
    method.setAccessible(true)
    val stage = method.invoke(ds, rdd, new Integer(rdd.partitions.size), None, new Integer(jobId), rdd.context.getCallSite).asInstanceOf[Stage]
    stage
  }
  
  /**
   * 
   */
  private def getShuffleMapStage(shuffleDep: ShuffleDependency[_, _, _], stage: Stage): Stage = {
    val ds = stage.rdd.context.dagScheduler
    val method = ds.getClass.getDeclaredMethods().filter(_.getName().endsWith("getShuffleMapStage"))(0)
    method.setAccessible(true)
    val stages = method.invoke(ds, shuffleDep, stage.jobId.asInstanceOf[Object]).asInstanceOf[Stage]
    stages
  }

  /**
   *
   */
  def persist(sc: SparkContext, rdd: RDD[_], newLevel: StorageLevel):RDD[_] = {
    logDebug("Caching " + rdd)
    
    val cachedRdd =
      if (rdd.name != null) {    
        val path = new Path(rdd.name)
        if (fs.exists(path)) {
          rdd
        } else {
          this.doPersist(sc, rdd, newLevel)
        }
      } else {
        this.doPersist(sc, rdd, newLevel)
      } 
    cachedRdd
  }

  /**
   *
   */
  private def doPersist(sc: SparkContext, rdd: RDD[_], newLevel: StorageLevel): RDD[_] = {
    sc.persistRDD(rdd)
    sc.cleaner.foreach(_.registerRDDForCleanup(rdd))
    rdd
  }
  
  /**
   * 
   */
  def unpersist(sc: SparkContext, rdd: RDD[_], blocking: Boolean = true): RDD[_] = {
    sc.unpersistRDD(rdd.id, blocking)
    val path = new Path(sc.appName + "/cache/cache_" + rdd.id)
    fs.delete(path, true)
    rdd
  }
}