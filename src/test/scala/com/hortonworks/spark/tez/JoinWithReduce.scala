package com.hortonworks.spark.tez

import org.apache.spark.Tez
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.tez.TezDelegatingExecutor

/**
 * This sample is similar to SampleJoin.scala and demonstrates join between two sources
 * However, unlike the SampleJoin.scala it reduces (merges) output by key and then sorts its which results
 * in additional stage making it 4 stage (4 Vertexes) DAG
 *
 * First two stages will read and map the file and their output will be read
 * by a third stage and eventually fed into a fourth stage to do the sorting
 */
object JoinWithReduce extends App {
//  val fs = FileSystem.get(new YarnConfiguration);
//  var testFile = new Path("file1.txt");
//  fs.copyFromLocalFile(false, true, new Path("/Users/ozhurakousky/dev/spark-on-tez/file1.txt"), testFile);
//  testFile = new Path("file2.txt");
//  fs.copyFromLocalFile(false, true, new Path("/Users/ozhurakousky/dev/spark-on-tez/file2.txt"), testFile);
//
//  val sc = new SparkContext("external:" + classOf[TezDelegatingExecutor].getName, "SparkOnTez-joinReduceSort") 
//
//  val s1 = sc.textFile("file1.txt")
//  val s2 = sc.textFile("file2.txt")
//
//  val result = s1.map { line => val x = line.split("\\s+"); (x(2), x(0) + "," + x(1)) }
//    .join(s2.map { line => val x = line.split("\\s+"); (x(0), x(1)) })
//    .reduceByKey { (x, y) =>
//      println("reducing");
//      (x.toString, y.toString)
//    }
//    .sortByKey(true)
//    .collect
//
//  println(result.toList)
//
//  sc.stop
}