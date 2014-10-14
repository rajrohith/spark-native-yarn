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

/**
 *
 */
class TezJobExecutionContext extends JobExecutionContext with Logging {
  SparkToTezAdapter.adapt

  private val cachedRDDs = new HashSet[Path]

  private val runJobInvocationCounter = new AtomicInteger

  private val tezDelegate = new TezDelegate

  private val fs = FileSystem.get(tezDelegate.tezConfiguration)

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
    throw new UnsupportedOperationException("Broadcast not supported yet")
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

    val (operationName, finalRdd) = this.computeLastRdd(sc, rdd)
    val stage = this.caclulateStages(sc, finalRdd)

    this.tezDelegate.submitApplication[T, U](sc.appName, sc.hadoopConfiguration, stage: Stage, func: (TaskContext, Iterator[T]) => U)

    this.processResult(sc.appName, partitions.size, operationName, returnType, resultHandler)
  }

  /**
   *
   */
  private def processResult[U](appName: String, partitionSize: Int, operationName: String, returnType: ClassTag[U], resultHandler: (Int, U) => Unit) {
    if (!classOf[Unit].isAssignableFrom(returnType.runtimeClass)) {
      logDebug("Building non Unit result")
      val conf = new TezConfiguration
      val fs = FileSystem.get(conf);
      val iter = fs.listFiles(new Path(appName + "_out"), false);
      var partitionCounter = 0
      while (iter.hasNext() && partitionCounter < partitionSize) {
        val fStatus = iter.next()
        if (!fStatus.getPath().toString().endsWith("_SUCCESS")) {
          if (operationName == "count") {
            val reader = new BufferedReader(new InputStreamReader(fs.open(fStatus.getPath())))
            val line = reader.readLine()
            if (line == null){
              throw new IllegalStateException("Failed to read the 'count' result from " + fStatus.getPath())
            }
            val count = Long.parseLong(line.split("\\s+")(1))
            resultHandler(partitionCounter, count.asInstanceOf[U])
            reader.close
          }
          partitionCounter += 1
        }
      }
    }
  }

  /**
   *
   */
  private def validateIfSupported(operationName: String) {
    if (operationName == "collect") {
      throw new UnsupportedOperationException("'collect' is not supported yet")
    }
  }

  /**
   *
   */
  private def computeLastRdd(sc: SparkContext, rdd: RDD[_]): Tuple2[String, RDD[_]] = {
    val operationName = SparkUtils.getLastMethodName
    val (fName, fRdd) =
      if (operationName == "count") {
        (operationName, rdd.map(x => ("l", 1)).reduceByKey(_ + _))
      } else {
        (operationName, rdd)
      }
    this.validateIfSupported(fName)
    (fName, fRdd)
  }

  /**
   *
   */
  private def caclulateStages(sc: SparkContext, rdd: RDD[_]): Stage = {
    val ds = sc.dagScheduler
    val method = ds.getClass.getDeclaredMethod("newStage", classOf[RDD[_]], classOf[Int], classOf[Option[ShuffleDependency[_, _, _]]], classOf[Int], classOf[CallSite])
    method.setAccessible(true)
    val stage = method.invoke(ds, rdd, new Integer(1), None, new Integer(0), sc.getCallSite).asInstanceOf[Stage]
    stage
  }

  /**
   *
   */
  def persist(sc: SparkContext, rdd: RDD[_], newLevel: StorageLevel): RDD[_] = {
    logDebug("Caching " + rdd)
    val cachePath = rdd.name
    if (cachePath != null && this.cachedRDDs.contains(this.fs.makeQualified(new Path(cachePath)))) {
      logInfo("Skipping caching as the RDD is already cached")
      val cachedRdd = sc.textFile(cachePath)
      cachedRdd
    } else {
      val outputDirectory = sc.appName + "_cache_" + rdd.id
      rdd.saveAsTextFile(outputDirectory)

      val cachedRdd = new TezRDD(outputDirectory, sc, classOf[TextInputFormat],
        classOf[Text], classOf[IntWritable], new TezConfiguration)
      logInfo("Cached RDD: " + cachedRdd)
      cachedRDDs += cachedRdd.getPath
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