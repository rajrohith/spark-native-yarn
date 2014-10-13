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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.tez.TezJobExecutionContext
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

/**
 * This demo demonstrates one of the rudimentary Hadoop use case - counting unique words
 *
 */
object UniqueWordCountWithCollect {

  def main(args: Array[String]) {
    val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName
    val sc = new SparkContext(masterUrl, "UniqueWordCount")
    val source = sc.textFile("/Users/ozhurakousky/dev/fork/stark/src/demo/scala/dev/demo/test.txt")
    val result = source
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y, 2)
      .saveAsNewAPIHadoopFile("out", classOf[Text],
        classOf[IntWritable], classOf[TextOutputFormat[_, _]])
    sc.stop
  }

  def run(inputFile: String, reducers: Int) {
    //prep the job by copying the artifacts to HDFS
    val jobName = DemoUtilities.prepareJob(Array(inputFile))
    val outputPath = jobName + "_out"

    //create the SparkContext and read the file
    val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName
    val sc = new SparkContext(masterUrl, "UniqueWordCount")
    val source = sc.textFile(inputFile)

    //process it
    val result = source
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y, reducers)
      .persist

    //    result
    //      .flatMap(x => x.split(" "))
    //      .map(x => (x, 1))
    //      .reduceByKey((x, y) => x + y, reducers)

    //    result.count

    //cleanup
    sc.stop()

    DemoUtilities.printSampleResults(outputPath)
  }
}