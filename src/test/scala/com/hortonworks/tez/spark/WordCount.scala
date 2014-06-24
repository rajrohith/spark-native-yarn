package com.hortonworks.tez.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCount extends App {

  val sc = new SparkContext("local", "foo")
  val file = sc.textFile("/Users/ozhurakousky/sample.txt") 
  println(file.name)

  val rdd = file.flatMap{line => println("flatMap"); line.split(" ")}
                 .map{word => (word, 1)}
//                 .filter{_ => false}
                 .reduceByKey(_ + _)
                 .collect
                 
  for (r <- rdd) {
    println(r)
  }
}