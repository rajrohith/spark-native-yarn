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
import java.util.concurrent.Executors
import java.util.concurrent.CountDownLatch
import org.apache.spark.tez.TezConstants
import org.apache.spark.SparkConf
import java.util.concurrent.atomic.AtomicInteger

/**
 * This demo demonstrates one of the rudimentary ETL use cases such as partitioning source data.
 * Similar to the 'reduceBy', the data will be partitioned via 'partitionBy' and then
 * written as file-per-key into as many files as there are keys in the source file using
 * custom implementation of MultipleTextOutputFormat.
 *
 */
object PartitionByMulti {

  def main(args: Array[String]) {
    var reducers = 1
    var inputFile = "/user/hive/external/tpch-100/lineitem "
    if (args != null && args.length > 0) {
      reducers = Integer.parseInt(args(0))
      if (args.length > 1) {
        inputFile = args(1)
      }
    }

    System.setProperty(TezConstants.GENERATE_JAR, "true");
    System.setProperty(TezConstants.UPDATE_CLASSPATH, "false");
    val tenants = 2
    val executor = Executors.newCachedThreadPool()
    val latch = new CountDownLatch(tenants)
    for (i <- 0 until tenants) {   
      executor.execute(new Runnable {
        def run() {
          try {
            val jobName = "PartitionByMulti_" + System.currentTimeMillis()
            val outputPath = jobName + "_out"

            val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName
            val conf = new SparkConf
            conf.set("spark.ui.enabled", "false");
            conf.setMaster(masterUrl)
            conf.setAppName(jobName)
            val sc = new SparkContext(conf)
            val source = sc.textFile(inputFile)

            val result = source
              .map(a => (a.split('|')(10), a))
              .partitionBy(new HashPartitioner(32))
              .saveAsHadoopFile(outputPath, classOf[Text], classOf[Text], classOf[KeyPerPartitionOutputFormat])

            sc.stop
          } catch {
            case e: Throwable => e.printStackTrace()
          } finally {
            latch.countDown()
          }
        }
      });
//      Thread.sleep(3000);
    }

    latch.await()
    executor.shutdown()
    println("DONE")
    //DemoUtilities.printSampleResults(outputPath)
  }
}