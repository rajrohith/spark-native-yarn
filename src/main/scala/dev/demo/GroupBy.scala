package dev.demo

import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.tez.TezJobExecutionContext

/**
 * This demo demonstrates one of the rudimentary ETL use cases such as partitioning source data.
 * Similar to the 'reduceBy', the data will be partitioned via 'groupBy' and then 
 * written as file-per-key into as many files as there are keys in the source file using 
 * custom implementation of MultipleTextOutputFormat.
 * 
 */
object GroupBy {

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

    val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName
    val sc = new SparkContext(masterUrl, "foo")
    val source = sc.textFile(inputFile)

    val result = source
    	.map{s => val split = s.split("\\s+", 2); (split(0).replace(":", "_"), split(1))}
        .groupByKey(2)
    	.saveAsHadoopFile(outputPath, classOf[Text], classOf[Text], classOf[KeyPerPartitionOutputFormat])

    sc.stop

    DemoUtilities.printSampleResults(outputPath)
  }
}