package com.hortonworks.spark.tez

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.tez.instrument.TezInstrumentationAgent
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import org.apache.hadoop.io.LongWritable
import java.util.concurrent.atomic.AtomicInteger

object WordCountStark extends App {
  TezInstrumentationAgent.instrument

  val file = "100gb"
  val reducers = 50
  val appName = "stark_wc_" + file + "_" + reducers
//  val fileName = "test-file.txt"
    val fileName = "hdfs://cn105-10:8020/user/zzhang/" + file + ".txt"

  val counter = 10
  val inputFile = fileName
  

  val exeService = Executors.newCachedThreadPool();
  val latch = new CountDownLatch(counter)
  val start = System.currentTimeMillis();
  var port = new AtomicInteger(33333)
  for (i <- 0 until counter) {
    exeService.execute(new Runnable() {
      
      def run() {
        val sConf = new SparkConf
        sConf.set("spark.ui.port", port.incrementAndGet()+"")
//        sConf.set("spark.master", "local")
        sConf.setAppName("Stark-" + i)
        sConf.setMaster("local")
        val sc = new SparkContext(sConf)
        println("##### STARTING JOB");
        val source = sc.textFile(fileName)

        val result = source.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _, reducers).collect

        println("######## FINISHED: " + i)

        sc.stop
        latch.countDown()
      }
    })
//    Thread.sleep(10000);
  }
  println("Done submitting")
  latch.await();
  val stop = System.currentTimeMillis();
  println("STOPPED: " + (stop - start))

  exeService.shutdown();
}