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
package org.apache.spark.tez.adapter

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.TaskContext
import org.apache.hadoop.io.Writable
import org.apache.spark.Logging
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.Partitioner
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.tez.io.ValueWritable
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.Aggregator
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.serializer.Serializer
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkException
import org.apache.spark.InterruptibleIterator

/**
 * This is a template file which defines operations use during instrumentation(implementation swap) of
 * PairRDDFunctions class (see SparkToTezAdapter). Outside of instrumentation
 * this code is not used anywhere.
 */
object PairRDDFunctionsAdapter {
  /**
   * 
   */
  def saveAsHadoopDataset(config: JobConf, prdd:PairRDDFunctions[_,_]) {
    val thisConf = new JobConf(config)
    val fields = prdd.getClass().getDeclaredFields().filter(_.getName().endsWith("self"))
    val field = fields(0)
    field.setAccessible(true)
    val self: RDD[_] = field.get(prdd).asInstanceOf[RDD[_]]

    val outputFormat = thisConf.getClass("mapred.output.format.class", null)
    self.context.hadoopConfiguration.setClass("mapreduce.job.outputformat.class", outputFormat, classOf[org.apache.hadoop.mapred.OutputFormat[_, _]])

    val keyType = thisConf.getClass("mapreduce.job.output.key.class", null)
    self.context.hadoopConfiguration.setClass("mapreduce.job.output.key.class", keyType, classOf[Writable])

    val valueType = thisConf.getClass("mapreduce.job.output.value.class", null)
    self.context.hadoopConfiguration.setClass("mapreduce.job.output.value.class", valueType, classOf[Writable])

    var outputPath = thisConf.get("mapred.output.dir")
    if (outputPath == null || outputPath.trim().length() == 0) {
      outputPath = thisConf.get("mapreduce.output.fileoutputformat.outputdir")
    }
    self.context.hadoopConfiguration.set("mapred.output.dir", outputPath)

    self.context.runJob(self, (context: TaskContext, iter: Iterator[_]) => ())
  }
  
  /**
   * 
   */
  def saveAsNewAPIHadoopDataset(conf: Configuration, prdd:PairRDDFunctions[_,_]) {
    val fields = prdd.getClass().getDeclaredFields().filter(_.getName().endsWith("self"))
    val field = fields(0)
    field.setAccessible(true)
    val self: RDD[_] = field.get(prdd).asInstanceOf[RDD[_]]

    val outputFormat = conf.getClass("mapreduce.job.outputformat.class", null)
    self.context.hadoopConfiguration.setClass("mapreduce.job.outputformat.class", outputFormat, classOf[org.apache.hadoop.mapreduce.OutputFormat[_, _]])

    val keyType = conf.getClass("mapreduce.job.output.key.class", null)
    self.context.hadoopConfiguration.setClass("mapreduce.job.output.key.class", keyType, classOf[Writable])

    val valueType = conf.getClass("mapreduce.job.output.value.class", null)
    self.context.hadoopConfiguration.setClass("mapreduce.job.output.value.class", valueType, classOf[Writable])

    var outputPath = conf.get("mapred.output.dir")
    if (outputPath == null || outputPath.trim().length() == 0){
      outputPath = conf.get("mapreduce.output.fileoutputformat.outputdir")
    }
    self.context.hadoopConfiguration.set("mapred.output.dir", outputPath)

    self.context.runJob(self, (context: TaskContext, iter: Iterator[_]) => ())
  }
  
  /**
   * 
   */
  def combineByKey[K, C,V](prdd:PairRDDFunctions[_,_],
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    partitioner: Partitioner,
    mapSideCombine: Boolean = true,
    serializer: Serializer = null): RDD[(K, C)] = {
    require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0

    val fields = prdd.getClass().getDeclaredFields().filter(_.getName().endsWith("self"))
    val field = fields(0)
    field.setAccessible(true)
    val self: RDD[Product2[K, V]] = field.get(prdd).asInstanceOf[RDD[Product2[K, V]]]

    val aggregator = new Aggregator[K, V, C](createCombiner, mergeValue, mergeCombiners)
//    if (self.partitioner == Some(partitioner)) {
//      self.mapPartitions(iter => {
//        val context = TaskContext.get()
//        new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
//      }, preservesPartitioning = true)
//    } else {
      new ShuffledRDD[K, V, C](self, partitioner)
        .setSerializer(serializer)
        .setAggregator(aggregator)
        .setMapSideCombine(mapSideCombine)
//    }
//    new ShuffledRDD[K, V, C](self, partitioner)
//      .setSerializer(serializer)
//      .setAggregator(aggregator)
//      .setMapSideCombine(mapSideCombine)
  }
}

/**
 * Delegating adapter which provides instrumentation overrides for selected PairRDDFunctions methods.
 * The actual invocations are delegated to its companion object mainly to simplify debugging.
 */
class PairRDDFunctionsAdapter[K, V] extends Logging {

  /**
   * 
   */
  def saveAsNewAPIHadoopDataset(conf: Configuration) {
     PairRDDFunctionsAdapter.saveAsNewAPIHadoopDataset(conf, this.asInstanceOf[PairRDDFunctions[_,_]])
  }

  /**
   * 
   */
  def saveAsHadoopDataset(conf: JobConf) {
    PairRDDFunctionsAdapter.saveAsHadoopDataset(conf, this.asInstanceOf[PairRDDFunctions[_,_]])
  }

  /**
   * 
   */
  def combineByKey[C](createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    partitioner: Partitioner,
    mapSideCombine: Boolean = true,
    serializer: Serializer = null): RDD[(K, C)] = {
    PairRDDFunctionsAdapter.combineByKey(this.asInstanceOf[PairRDDFunctions[_,_]], createCombiner, 
        mergeValue, mergeCombiners, partitioner, mapSideCombine, serializer)
  }
}