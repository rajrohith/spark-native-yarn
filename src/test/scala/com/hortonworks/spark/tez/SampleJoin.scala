package com.hortonworks.spark.tez

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.Tez
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.RangePartitioner

object SampleJoin extends App {
  
  val fs = FileSystem.get(new YarnConfiguration);
  var testFile = new Path("file1.txt");
  fs.copyFromLocalFile(false, true, new Path("/Users/ozhurakousky/dev/spark-on-tez/file1.txt"), testFile);
  testFile = new Path("file2.txt");
  fs.copyFromLocalFile(false, true, new Path("/Users/ozhurakousky/dev/spark-on-tez/file2.txt"), testFile);
  
  val sc = new SparkContext("local", "SampleJoin") with Tez
  
  val s1 = sc.textFile("file1.txt")
  val s2 = sc.textFile("file2.txt")

  val result = s1.map{line => val x = line.split("\\s+"); (x(2), x(0) + "," +  x(1) )}
  	.join(s2.map{line => val x = line.split("\\s+"); (x(0), x(1))})
  	.reduceByKey{(x, y) => 
  	  println("reducing");
  	  (x.toString, y.toString)}
  	.sortByKey(true)
  	.collect
  	
  println(result.toList) 
}