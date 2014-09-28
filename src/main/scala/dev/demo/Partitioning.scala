package dev.demo

import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable

/**
 *
 */
object Partitioning {

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

    val jobName = DemoUtilities.prepareJob(Array(inputFile))
    val outputPath = jobName + "_out"

    val sc = new SparkContext()
    val source = sc.textFile(inputFile)

    val result = source
    	.map{s => val split = s.split("\\s+", 2); (split(0).replace(":", "_"), split(1))}
    	.partitionBy(new HashPartitioner(2))
    	.saveAsHadoopFile(outputPath, classOf[Text], classOf[Text], classOf[MyMultipleTextOutputFormat])

    sc.stop

    DemoUtilities.printSampleResults(outputPath)
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