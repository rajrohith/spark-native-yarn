package com.hortonworks.tez.spark

import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.OneToOneDependency

trait Tez extends SparkContext {
  
  /**
   * 
   */
  override def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    println("Intercepted runJob")
    val dependencies = flatten(rdd)
    println(dependencies)
    for (dependency <- dependencies) {
      println(dependency)
    }
    
    Array[U]()
  }

  /**
   * 
   */
  def flatten(rdd: RDD[_], l:List[_] = Nil): List[_] = {
    if (rdd.dependencies.size > 0) {
      flatten(rdd.dependencies(0).rdd, rdd :: l)
    }
    else {
      rdd :: l
    }
  }
  
}