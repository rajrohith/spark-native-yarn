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
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, Job => NewHadoopJob}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => NewFileOutputFormat}
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


class TezJobExecutionContext extends JobExecutionContext with Logging {
  SparkToTezAdapter.adapt
  
  private val tezConfig = new TezConfiguration
  private val fs = FileSystem.get(tezConfig)
  
  private val cachedRDDs = new HashSet[Path]
 /**
  * 
  */
  def hadoopFile[K, V](
    sc:SparkContext,
    path: String,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int = 1): RDD[(K, V)] = {
    
    val fs = FileSystem.get(tezConfig);
    val qualifiedPath = fs.makeQualified(new Path(path))
    if (!fs.exists(qualifiedPath)){
      throw new FileNotFoundException("Path: " + qualifiedPath + " does not exist")
    }
    
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

    val fs = FileSystem.get(tezConfig);
    val qualifiedPath = fs.makeQualified(new Path(path))
    if (!fs.exists(qualifiedPath)){
      throw new FileNotFoundException("Path: " + qualifiedPath + " does not exist")
    }

    new TezRDD(path, sc, fClass, kClass, vClass, conf)
  }

  /**
   * 
   */
  def broadcast[T: ClassTag](sc:SparkContext, value: T): Broadcast[T] = {
    throw new UnsupportedOperationException("Broadcast not supported yet")
  }

  /**
   * 
   */
  def runJob[T,U](
    sc:SparkContext,
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    allowLocal: Boolean,
    resultHandler: (Int, U) => Unit)(implicit returnType:ClassTag[U]) = {
    
    val outputMetadata = this.extractOutputMetedata(sc.hadoopConfiguration, sc.appName)
    if (outputMetadata == null){
      throw new IllegalArgumentException("Failed to determine output metadata (KEY/VALUE/OutputFormat type)")
    }
    logInfo("Will save output as " + outputMetadata)
    
    logDebug("RETURN TYPE for Job: " + returnType)
    
    val (operationName, finalRdd) = this.computeLastRdd(sc, rdd)
    
    this.validateIfSupported(operationName)

    val hadoopConfiguration = this.tezConfig
    logInfo("Default FS Address: " + hadoopConfiguration.get("fs.defaultFS"))
    logInfo("RM Host Name: " + hadoopConfiguration.get("yarn.resourcemanager.hostname"))
    logInfo("RM Address: " + hadoopConfiguration.get("yarn.resourcemanager.address"))
    logInfo("RM Scheduler Address: " + hadoopConfiguration.get("yarn.resourcemanager.scheduler.address"))
    logInfo("RM Resource Tracker Address: " + hadoopConfiguration.get("yarn.resourcemanager.resourcetracker.address"))
    
    val stage = this.caclulateStages(sc, finalRdd)
    val tezUtils = new Utils(stage, func)

    val dagTask: DAGTask = tezUtils.build(outputMetadata._1, outputMetadata._2, outputMetadata._3, outputMetadata._4)
    dagTask.execute

    if (!classOf[Unit].isAssignableFrom(returnType.runtimeClass)) {
      logDebug("Building non Unit result")
      val conf = new TezConfiguration
      val fs = FileSystem.get(conf);
      val iter = fs.listFiles(new Path(sc.appName + "_out"), false);
      var partitionCounter = 0
      while (iter.hasNext() && partitionCounter < partitions.size) {
        val fStatus = iter.next()
        if (!fStatus.getPath().toString().endsWith("_SUCCESS")) {
          if (operationName == "count") {
            val reader = new BufferedReader(new InputStreamReader(fs.open(fStatus.getPath())))
            val count = Long.parseLong(reader.readLine().split("\\s+")(1))
            resultHandler(partitionCounter, count.asInstanceOf[U])
            reader.close
          } 

          partitionCounter += 1
        }
      }
    }
  }
  
  private def validateIfSupported(operationName:String) {
    if (operationName == "collect"){
      throw new UnsupportedOperationException("'collect' is not supported yet")
    }
  }
  
  /**
   * 
   */
  private def computeLastRdd(sc:SparkContext, rdd:RDD[_]):Tuple2[String, RDD[_]] = {
    val operationName = SparkUtils.getLastMethodName
    if (operationName == "count"){
      (operationName, rdd.map(x => ("l",1)).reduceByKey(_ + _))
    }
    else {
      (operationName, rdd)
    }
  }
  
  /**
   * 
   */
  private def caclulateStages(sc:SparkContext, rdd:RDD[_]):Stage = {
    val ds = sc.dagScheduler
    val method = ds.getClass.getDeclaredMethod("newStage", classOf[RDD[_]], classOf[Int], classOf[Option[ShuffleDependency[_, _, _]]], classOf[Int], classOf[CallSite])
    method.setAccessible(true)
    val stage = method.invoke(ds, rdd, new Integer(1), None, new Integer(0), sc.getCallSite).asInstanceOf[Stage]
    stage
  }
  
  /**
   * 
   */
  private def extractOutputMetedata[T,U](conf:Configuration, appName:String):Tuple4[Class[_], Class[_], Class[_], String] = {  
    val outputFormat = conf.getClass("mapreduce.job.outputformat.class", classOf[TextOutputFormat[_,_]])
    val keyType = conf.getClass("mapreduce.job.output.key.class", Class.forName("org.apache.spark.tez.io.KeyWritable"))
    val valueType = conf.getClass("mapreduce.job.output.value.class", Class.forName("org.apache.spark.tez.io.ValueWritable"))
    val outputPath = conf.get("mapred.output.dir", appName + "_out")
    conf.clear()
    if (outputPath == null || outputFormat == null || keyType == null){
      null
    }
    else {
      (keyType, valueType, outputFormat, outputPath)
    }
    
  }
  
  def persist(sc:SparkContext, rdd:RDD[_], newLevel: StorageLevel):RDD[_] = {
    logDebug("Caching " + rdd)
    val cachePath = rdd.name
    if (cachePath != null && this.cachedRDDs.contains(this.fs.makeQualified(new Path(cachePath)))) {
      logInfo("Skipping caching as the RDD is already cached")
      val cachedRdd = sc.textFile(cachePath)
      cachedRdd
    } else {
      val outputDirectory = sc.appName + "_cache_" + rdd.id
      val outputPath = this.fs.makeQualified(new Path(outputDirectory))
      rdd.saveAsTextFile(outputDirectory)
      logDebug("Cached RDD in " + outputPath)
      val cachedRdd = sc.textFile(outputDirectory)
      logInfo("Cached RDD: " + cachedRdd)
      cachedRDDs += outputPath
      cachedRdd
    }
  }

  def unpersist(sc: SparkContext, rdd: RDD[_], blocking: Boolean = true): RDD[_] = {
    val cachePath = rdd.name
    if (cachePath != null) {
      val path = fs.makeQualified(new Path(cachePath))
      if (this.cachedRDDs.contains(path)) {
        fs.delete(path, true)
        logInfo("Un-Cached RDD: " + rdd + " from " + path)
        cachedRDDs -= path
      } else {
        logInfo("Skipping un-caching as the RDD is not cached")
      }
    }
    rdd
  }
}