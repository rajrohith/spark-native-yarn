package dev.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Join extends BaseDemo {

  def main(args: Array[String]) {
    val jobName = "Join"
    val file1 = "src/test/scala/dev/demo/file1.txt"
    val file2 = "src/test/scala/dev/demo/file2.txt"
    prepare(jobName, Array(file1, file2))

    val sConf = new SparkConf
    sConf.setAppName(jobName)
    sConf.setMaster("local")
    val sc = new SparkContext(sConf)

    val source1 = sc.textFile(file1)
    val source2 = sc.textFile(file2)

    val two = source2.map { x =>
      val s = x.split(" ")
      val key: Int = Integer.parseInt(s(0))
      (key, s(1))
    }

    val result = source1.map { x =>
      val s = x.split(" ")
      val key: Int = Integer.parseInt(s(2))
      val t = (key, (s(0), s(1)))
      t
    }.join(two).reduceByKey{(x, y) => 
      println("REDUCING!!!!!!!!!")
      ((x._1.toString, y._1.toString), x._2)}.collect

    println(result.toList)
    sc.stop
  }
}