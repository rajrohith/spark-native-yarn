package com.hortonworks.spark.tez

import scala.reflect.ClassTag
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import java.util.concurrent.Executors
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

object WordCountSpark extends App {

  val counter = Integer.parseInt(args(0))
  val inputFile = args(1)
  val reducers = Integer.parseInt(args(2))
  //  val outputFile = args(2)
  val exeService = Executors.newCachedThreadPool();
  val latch = new CountDownLatch(counter)
  val start = System.currentTimeMillis();
  var port = new AtomicInteger(33333)
  for (i <- 0 until counter) {
    
    exeService.execute(new Runnable() {
      def run() {
        val sConf = new SparkConf
        sConf.set("spark.ui.port", port.incrementAndGet() + "")
        sConf.setAppName("Stark-" + i)
        sConf.setMaster("yarn-client")
        val sc = new SparkContext(sConf)
//        val sc = new SparkContext("yarn-client", "SparkYARN-" + i)

        try {
          println("##### STARTING JOB");
          val source = sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](inputFile).map(pair => pair._2.toString).setName(inputFile)

          val result = source.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _, reducers).saveAsTextFile(start + "-" + i)

          println("######## FINISHED: " + i)
        } catch {
          case y: Throwable =>
            y.printStackTrace()
        } finally {
          sc.stop
          latch.countDown()
        }

      }
    })
//    Thread.sleep(10000)
  }
  println("Done submitting")
  latch.await();
  val stop = System.currentTimeMillis();
  println("STOPPED: " + (stop - start))

  exeService.shutdown();
}