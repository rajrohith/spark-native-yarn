package dev.demo

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.IntWritable

/**
 * This demo demonstrates one of the rudimentary Hadoop use case - WordCount
 * 
 */
object WordCount {

  def main(args: Array[String]) {
    var reducers = 1
    var inputFile = "src/main/scala/dev/demo/test.txt"
    if (args != null && args.length > 0) {
      reducers = Integer.parseInt(args(0))
      if (args.length > 1) {
        inputFile = args(1)
      }
    }
    println("Will execute WordCount on file: " + inputFile + " with reducers " + reducers)
    wordCount(inputFile, reducers);
  }

  def wordCount(inputFile: String, reducers: Int) {
    //prep the job by copying the artifacts to HDFS
    val jobName = DemoUtilities.prepareJob(Array(inputFile))
    val outputPath = jobName + "_out"

    //create the SparkContext and read the file
    val sc = new SparkContext()
    val source = sc.textFile(inputFile)

    //process it
    val result = source
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y, reducers)
      .saveAsNewAPIHadoopFile(outputPath, classOf[Text],
        classOf[IntWritable], classOf[TextOutputFormat[_, _]])

    //cleanup
    sc.stop()

    DemoUtilities.printSampleResults(outputPath)
  }
}