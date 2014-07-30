package com.hortonworks.spark.tez

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.Tez

object SampleMapReduce extends App {
  
//  val fs = FileSystem.get(new YarnConfiguration);
//  val testFile = new Path("sample-256.txt");
//  fs.copyFromLocalFile(false, true, new Path("/Users/ozhurakousky/dev/spark-on-tez/sample-256.txt"), testFile);
//  println(fs.makeQualified(testFile))
  
  val sc = new SparkContext("local", "SparkOnTez") with Tez
  
  println("##### STARTING JOB");
  
  val source = sc.textFile("sample-256.txt")
  val result = source
    .flatMap ( line => line.split(" ") )
    .map(word => (word, 1L))
    .filter(x => x._1 != "foo")
    .reduceByKey(_ + _, 4)
    .collect

    println("######## FINISHED")
  println(result.toSet)
    
  def combine = (x: Int, y: Int) => {
    x + y
  }
  
  def reduce = (x: Int, y: Int) => {
    x + y
  }
}
