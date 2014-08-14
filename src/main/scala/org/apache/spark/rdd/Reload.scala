package org.apache.spark.rdd

import scala.collection.Map

import org.apache.spark.SparkContext
import org.apache.spark.tez.rdd.ParallelCollectionRDD_override

import com.hortonworks.spark.tez.utils.ClassLoaderUtils

object Reload extends App {
//  val sc = new SparkContext("local", "SparkOnTez-WordCount") 
  ClassLoaderUtils.loadClassAs(classOf[ParallelCollectionRDD_override[_]], "org.apache.spark.rdd.ParallelCollectionRDD");
  new ParallelCollectionRDD(new SparkContext("local", "SparkOnTez-WordCount"), Nil, 1, Map[Int, Seq[String]]())
//  new PCRDD[String](new SparkContext("local", "SparkOnTez-WordCount"), Nil, 1, Map[Int, Seq[String]]())
}