package com.hortonworks.spark.tez

import scala.Array.canBuildFrom

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object MapReduce extends App {
 
  val sc = new SparkContext("local", "SparkOnTez") 
 
  val source = sc.textFile("sample-256.txt")
  
//  val source = sc.parallelize(List("Every and member and Congress swore an oath to people", "American people and their elected"), 2)

//  	val s:Option[String] = None
  
    val result = source
      .flatMap {line => for (x <- line.split(" ")) yield new Tuple2[String, Int](x, 1)}
      .reduceByKey((_ + _), 4)
  	  .collect
  	  
  	 
  	  
  	  println(result.toSet)

  	  
//  	  val s = new MyDAGScheduler
//  	  val stage = s.newStage(result, 2, None, 2, None)
//  	  val pStage = stage.parents(0)
//  	  val pRdd = pStage.rdd
//  	  val fField = pRdd.getClass().getDeclaredField("f")
//  	  fField.setAccessible(true)
//  	  val function:Function3[Any, Any, Any, Any] =  fField.get(pRdd).asInstanceOf[Function3[Any, Any, Any, Any]]
//  	  println(function)
//  	  
//  	  val i = List((1, "Every and member and Congress swore an oath to people"), (2, "American people and their elected")).iterator
//  	  
//  	  
//  	  
//  	  val r = function(null, 1, i)
//  	  println(r)
  	  
//  	  val scheduler = new DAGScheduler(null, null, null, null, null, null)
  		
//  	val myStage = new MyStage(0, result, 1, None, Nil, 5, None)
//  	
//  	println(myStage)
//      .reduceByKey {
//        (x, y) => 
//          println(Thread.currentThread().getId() + "====> REDUCE: x: " + x + "; y: " + y); 
//          x + y
//      }
//  	  .map((v) => v)
//  	  .reduceByKey(_ + _)
//      .collect
      
//      println(result.toSeq)

//  val result = source.flatMap { line =>
////    println(Thread.currentThread().getId() + "==> FLAT MAP: " + line);
//    line.split(" ")
//  }
//    .map { word =>
////      println(Thread.currentThread().getId() + "====> MAP:" + word);
//      (word, 1)
//    }
//    reduceByKey
////    .combineByKey[Int]{(v:Int) => v, combine, reduce, new HashPartitioner(16)}
////    .map {tuple => 
//////      	println("TUPLE: " + tuple); 
////    		tuple
////    }
////    .reduceByKey {
////      (x, y) =>
//////        println(Thread.currentThread().getId() + "====> REDUCE: x: " + x + "; y: " + y);
////        x + y
////    }
////    .map {tuple => 
////    		tuple
////    }
////    .reduceByKey((x, y) => x)
//    .collect
//  	
//  println("Result: " + result.toSet)

  def combine = (x: Int, y: Int) => {
//    println(Thread.currentThread().getId() + "====> combine: x: " + x + "; y: " + y);
    x + y
  }
  
  def reduce = (x: Int, y: Int) => {
    println(Thread.currentThread().getId() + "====> reduce: x: " + x + "; y: " + y);
    x + y
  }
  

       
  //val smt = new ShuffMap
}