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
package dev.demo

import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.tez.TezJobExecutionContext
import org.apache.spark.tez.TezConstants

/**
 * This demo demonstrates one of the rudimentary ETL use cases such as partitioning source data.
 * Similar to the 'reduceBy', the data will be partitioned via 'partitionBy' and then 
 * written as file-per-key into as many files as there are keys in the source file using 
 * custom implementation of MultipleTextOutputFormat.
 * 
 */
object PartitionBy {

  def main(args: Array[String]) {
    var reducers = 1
    var inputFile = "src/main/scala/dev/demo/partitioning.txt"
    if (args != null && args.length > 0) {
      reducers = Integer.parseInt(args(0))
      if (args.length > 1) {
        inputFile = args(1)
      }
    }

    println("Will execute Partitioning on file: " + inputFile +
      " after copying it to HDFS")

    val jobName = DemoUtilities.prepareJob(Array(inputFile))
    val outputPath = jobName + "_out"

    val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName
    val sc = new SparkContext(masterUrl, "PartitionBy")
    val source = sc.textFile(inputFile)

    val result = source
    	.map{s => val split = s.split("\\s+", 2); (split(0).replace(":", "_"), split(1))}
    	.partitionBy(new HashPartitioner(2))
    	.saveAsHadoopFile(outputPath, classOf[Text], classOf[Text], classOf[KeyPerPartitionOutputFormat])

    sc.stop

    DemoUtilities.printSampleResults(outputPath)
  }
}


object PerfPartitionBy {

  def main(args: Array[String]) {
    System.setProperty(TezConstants.UPDATE_CLASSPATH, "true")
    var reducers = 1
    var inputFile = "src/main/scala/dev/demo/partitioning.txt"
    if (args != null && args.length > 0) {
      reducers = Integer.parseInt(args(0))
      if (args.length > 1) {
        inputFile = args(1)
      }
    }

    println("Will execute Partitioning on file: " + inputFile +
      " after copying it to HDFS")

    // val jobName = DemoUtilities.prepareJob(Array(inputFile))
    val outputPath = args(2)//jobName + "_out"

    val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName
    val sc = new SparkContext(masterUrl, "MyPartitionBy")
    val source = sc.textFile(inputFile)

    val result = source.map(a=>(a.split('|')(10), a))
      .partitionBy(new HashPartitioner(reducers))
      .saveAsHadoopFile(outputPath, classOf[Text], classOf[Text], classOf[KeyPerPartitionOutputFormat])

    sc.stop

    DemoUtilities.printSampleResults(outputPath)
  }
}

object SparkPartitionBy {

  def main(args: Array[String]) {
    System.setProperty(TezConstants.UPDATE_CLASSPATH, "true")
    var reducers = 1
    var inputFile = "src/main/scala/dev/demo/partitioning.txt"
    if (args != null && args.length > 0) {
      reducers = Integer.parseInt(args(0))
      if (args.length > 1) {
        inputFile = args(1)
      }
    }

    println("Will execute Partitioning on file: " + inputFile +
      " after copying it to HDFS")

    // val jobName = DemoUtilities.prepareJob(Array(inputFile))
    val outputPath = args(2)//jobName + "_out"

    val sc = new SparkContext()
    val source = sc.textFile(inputFile)

    val result = source.map(a=>(a.split('|')(10), a))
      .partitionBy(new HashPartitioner(reducers))
      .saveAsHadoopFile(outputPath, classOf[Text], classOf[Text], classOf[KeyPerPartitionOutputFormat])

    sc.stop

    DemoUtilities.printSampleResults(outputPath)
  }
}