package dev.demo

import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.tez.TezJobExecutionContext
import org.apache.spark.tez.io.ValueWritable
import org.apache.tez.dag.api.TezConfiguration
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.yarn.conf.YarnConfiguration
import scala.reflect.io.File
import org.apache.spark.HashPartitioner
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable

/**
 *
 */
object Partitioning extends BaseDemo {

  def main(args: Array[String]) {
    var reducers = 1
    var inputFile = "src/main/scala/dev/demo/partitioning.txt"
    if (args != null && args.length > 0) {
      reducers = Integer.parseInt(args(0))
      if (args.length > 1) {
        inputFile = args(1)
      }
    }

    println("Will execute Partitioning on file: " + inputFile +
      " after copying it to HDFS")

    val jobName = this.getClass().getName() + "-" + System.currentTimeMillis()
    val outputPath = jobName + "_out"
    prepare(jobName, Array(inputFile))

    val sConf = new SparkConf
    sConf.setAppName(jobName)
    val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName()
    sConf.setMaster(masterUrl)
    val sc = new SparkContext(sConf)
    val source = sc.textFile(inputFile)

    val result = source
    	.map{s => val split = s.split("\\s+", 2); (split(0).replace(":", "_"), split(1))}
    	.partitionBy(new HashPartitioner(2))
    	.saveAsHadoopFile(outputPath, classOf[Text], classOf[Text], classOf[MyMultipleTextOutputFormat])

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
              println("\t" + line)
              counter += 1
            }
          }
        }
      }
    }
  }
}

class MyMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any = {
     println("Generating Actual key for ")
    NullWritable.get()
  }

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    println("Generating File Name for ")
    key.asInstanceOf[String]
  }
}