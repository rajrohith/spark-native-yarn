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
    val jobName = DemoUtilities.prepareJob(Array())
    val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName
    val sc = new SparkContext(masterUrl, "jobName")
    val source = sc.parallelize(List("A", "B", "B", "A", "B", "A", "A", "A", "A", "A", "B", "A"), 4)

    val result = source
    	.filter(_ == "A")
    	.count

    println(result)
    sc.stop
  }
}