package dev.demo

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

object Join {

  def main(args: Array[String]) {
    val file1 = "src/main/scala/dev/demo/file1.txt"
    val file2 = "src/main/scala/dev/demo/file2.txt"
    val jobName = DemoUtilities.prepareJob(Array(file1, file2))
    val outputPath = jobName + "foo.nla_out"

    val sc = new SparkContext()

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
    }.join(two).reduceByKey { (x, y) =>
      println("REDUCING!!!!!!!!!") // good place to set a breakpoint when executing in mini-cluster to observe debug features
      ((x._1.toString, y._1.toString), x._2)
    }
      .saveAsNewAPIHadoopFile(outputPath, classOf[IntWritable], classOf[Text], classOf[TextOutputFormat[_, _]])
      
    sc.stop
    DemoUtilities.printSampleResults(outputPath)
  }
}