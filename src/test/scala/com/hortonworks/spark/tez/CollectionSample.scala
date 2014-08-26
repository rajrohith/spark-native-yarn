//package com.hortonworks.spark.tez
//
//import org.apache.spark.Tez
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//
//object CollectionSample extends App {
//
//  val sc = new SparkContext("local", "collections") with Tez
//  val source = sc.parallelize(List("oleg", "nastia", "seva", "oleg", "oleg", "seva", "oleg", "nastia"), 4)
//  
//  println()
//  
//  val result = source
//  	.map((_, 1L))
//  	.reduceByKey(_+_)
//  	.collect
//  	
//  println(result.toList)
//}