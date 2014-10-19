package org.apache.spark.tez

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import breeze.linalg.{Vector, DenseVector, squaredDistance}
import dev.demo.DemoUtilities
import org.apache.hadoop.io.NullWritable
import org.apache.spark.tez.io.KeyWritable
import org.apache.commons.io.FileUtils

object KMeans {
  
  /**
   * 
   */
  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(' ').map(_.toDouble))
  }

  /**
   * 
   */
  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 1
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }
  
  /**
   * 
   */
  def main(args:Array[String]) {
    
    val conf = this.buildSparkConf
    conf.setAppName("kmeans")
    val sc = new SparkContext(conf)
    
    val lines = sc.textFile("/Users/ozhurakousky/dev/fork/stark/src/test/scala/org/apache/spark/tez/kmeans_data.txt")

    val data = lines.map(parseVector _).cache
    
    val K = 4
    val convergeDist:Double = 0.0
    // look at issue with sample
    val kPoints = data.takeSample(withReplacement = false, K, 42).toArray
    
    
    var tempDist = 1.0

    while(tempDist > convergeDist) {
      val closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))

      val pointStats = closest.reduceByKey{case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)}

      val newPoints = pointStats.map {pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))}.collectAsMap()

      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += squaredDistance(kPoints(i), newPoints(i))
      }

      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      println("Finished iteration (delta = " + tempDist + ")")
    }

    println("Final centers:")
    kPoints.foreach(println)
    sc.stop()
  }
  
  /**
   *
   */
  def buildSparkConf(): SparkConf = {
    val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName
//    val masterUrl = "local"
    val sparkConf = new SparkConf
    sparkConf.set("spark.ui.enabled", "false")
    sparkConf.setMaster(masterUrl)
    sparkConf
  }
}