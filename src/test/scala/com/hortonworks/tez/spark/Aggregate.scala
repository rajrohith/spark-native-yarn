package com.hortonworks.tez.spark

import scala.collection.mutable.HashMap
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Will aggregate tuple values by key
 * Defines two functions:
 * 	mergeElement
 *  	Will create a new Map containing pairs of aggregated tuples
 *  mergeMaps
 *  	Will aggregate maps produced by mergeElement into a single map
 *   	This function will execute optionally. Typically with high level of parallelism
 *      there may be multiple 'mergeElement' instances executed on various parts of collection.
 *      In this case all those maps will have distinct tuples and will be merged, otherwise it will only execute once
 *
 *  This is essentially Map/Reduce done differently
 *
 *   Essentially on the collection of elements where mapping is not required all you need to do is
 *   execute 'reduceByKey' (see commented code at the bottom)
 */
object Aggregate extends App {

  val sc = new SparkContext("local", "SparkOnTez")

  val pairs = sc.parallelize(Array(("a", 1), ("b", 2), ("a", 2), ("a", 2), ("c", 5), ("a", 3)), 4)
  val pairs1 = sc.parallelize(Array(("a", 1), ("b", 2)), 4)
  val pairs2 = sc.parallelize(Array(("f", 1), ("z", 2)), 4)
  
 
  val cartesianResult = pairs1.cartesian(pairs2).collect
  println("Car: " + cartesianResult.toSet)
  
  val emptyMap = new HashMap[String, Int] {
    override def default(key: String): Int = 0
  }
  val mergeElement: (HashMap[String, Int], (String, Int)) => HashMap[String, Int] = (map, pair) => {
    //println("In mergeElement")
    map(pair._1) += pair._2
    //println("Map: " + map)
    map
  }
  val mergeMaps: (HashMap[String, Int], HashMap[String, Int]) => HashMap[String, Int] = (map1, map2) => {
    //println("In mergeMaps")
    for ((key, value) <- map2) {
      // println("key=" + key + " value=" + value)
      map1(key) += value
    }
    map1
  }
  val result = pairs.aggregate(emptyMap)(mergeElement, mergeMaps)
  
//  // in thi case flatMap is redundant
//  val result = pairs.flatMap(element => List(element)).reduceByKey(_ + _).collect
  //  	val result = pairs.reduceByKey(_ + _).collect
  println(result.toSet)
  
  val countResult = pairs.glom.collect.toSet
  println("RES: " + countResult)
}