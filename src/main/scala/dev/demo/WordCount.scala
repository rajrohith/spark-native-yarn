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

/**
 * This demo demonstrates one of the rudimentary Hadoop use case - WordCount
 * 
 */
object WordCount extends BaseDemo {

  def main(args: Array[String]) {
    var reducers = 1
    var inputFile = "src/main/scala/dev/demo/test.txt"
    if (args != null && args.length > 0) {
      reducers = Integer.parseInt(args(0))
      if (args.length > 1) {
        inputFile = args(1)
      }
    }

    println("Will execute WordCount on file: " + inputFile +
      " after copying it to HDFS using " + reducers + "  reducers")

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
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y, reducers)
      .saveAsNewAPIHadoopFile(outputPath, classOf[Text], classOf[IntWritable], classOf[TextOutputFormat[_, _]])
    /*
         * .saveAsTextFile(outputPath)
         * Will not work at the moment since its format is NullWritable/Text which will essentially go into Tez
         * thus creating incompatibility for key/value going into reduce
         */

    /*
         * .saveAsHadoopFile(outputPath, classOf[Text], classOf[IntWritable], classOf[org.apache.hadoop.mapred.TextOutputFormat[_,_]])
         * Does work 
         */
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