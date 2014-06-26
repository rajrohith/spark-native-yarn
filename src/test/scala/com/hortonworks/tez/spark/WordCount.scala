package com.hortonworks.tez.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.conf.YarnConfiguration
import com.hortonworks.spark.tez.Tez

object WordCount extends App {

  prepTestFile
  
  // =================================
  val sc = new SparkContext("local", "SparkOnTez") with Tez
  val source = sc.textFile("sample.txt")

  val rdd = source
  	.flatMap {line =>			// TokenProcessor
    		println("In Function 1");
    		println("Hello guys");
    		for (x <- line.split(" ")) yield new Tuple2[String, Int](x, 1)
  		}
    .reduceByKey(_ + _)			// SumProcessor
    .collect
    
   

  // =================================

  def prepTestFile() {
    try {
      val fs = FileSystem.get(new YarnConfiguration);
      val testFile = new Path("sample.txt");
      val out = fs.create(testFile, true);
      var i = 0;
      for (i <- 1 to 10000) {
        out.write("hello world ".getBytes());
      }
      out.close();
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}