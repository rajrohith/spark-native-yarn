package dev.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.tez.TezJobExecutionContext
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.io.BytesWritable
import java.io.BufferedReader
import org.apache.tez.dag.api.TezConfiguration
import org.apache.hadoop.fs.FileSystem
import java.io.InputStreamReader
import org.apache.hadoop.fs.Path

object Join extends BaseDemo {

  def main(args: Array[String]) {
    val jobName = "Join-" + System.currentTimeMillis()
    val file1 = "src/main/scala/dev/demo/file1.txt"
    val file2 = "src/main/scala/dev/demo/file2.txt"
    val outputPath = jobName + "foo.nla_out"
    prepare(jobName, Array(file1, file2))

    val sConf = new SparkConf
    sConf.setAppName(jobName)
    val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName()
    sConf.setMaster(masterUrl)
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
    }.join(two).reduceByKey { (x, y) =>
      println("REDUCING!!!!!!!!!") // good place to set a breakpoint when executing in mini-cluster to observe debug features
      ((x._1.toString, y._1.toString), x._2)
    }
      .saveAsNewAPIHadoopFile(outputPath, classOf[IntWritable], classOf[Text], classOf[TextOutputFormat[_, _]])
      
    sc.stop
    this.printSampleResults(outputPath)
  }

  /**
   *
   */
  def printSampleResults(outputPath: String) {
    val conf = new TezConfiguration
    val fs = FileSystem.get(conf);
    val iter = fs.listFiles(new Path(outputPath), false);
    var counter = 0;
    var run = true
    while (iter.hasNext() && run) {
      val status = iter.next();
      if (status.isFile()) {
        if (!status.getPath().toString().endsWith("_SUCCESS")) {
          println("Results from " + status.getPath() + " - " + fs.getLength(status.getPath()))

          val reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())))
          var line: String = null
          var r = true
          while (r && counter < 20) {
            line = reader.readLine()
            if (line == null) {
              r = false
            } else {
              println(line)
              counter += 1
            }
          }
        }
      }
    }
  }
}