package dev.demo

import org.apache.spark.tez.instrument.TezInstrumentationAgent
import org.apache.spark.tez.TezConstants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.tez.dag.api.TezConfiguration
import java.net.URLClassLoader
import java.net.URL
import java.io.File
import java.io.BufferedReader
import java.io.InputStreamReader

/**
 * 
 */
object WordCount extends BaseDemo {

  def main(args: Array[String]) {
    
    val jobName = "WordCount"
    val inputFile = "src/test/scala/dev/demo/sample-data.txt"
    prepare(jobName, inputFile)

    val sConf = new SparkConf
    sConf.setAppName(jobName)
    sConf.setMaster("local")
    val sc = new SparkContext(sConf)
    val source = sc.textFile(inputFile)

    val result = source.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _, 2).collect

    printResults(jobName)

    sc.stop
  }

}