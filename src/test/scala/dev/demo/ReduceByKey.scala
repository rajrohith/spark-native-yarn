package dev.demo

import org.apache.spark.tez.instrument.TezInstrumentationAgent
import org.apache.spark.tez.TezConstants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.tez.dag.api.TezConfiguration
import java.net.URLClassLoader
import java.net.URL
import java.io.File
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.spark.tez.TezJobExecutionContext
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

/**
 * 
 */
object ReduceByKey extends BaseDemo {

  def main(args: Array[String]) {
    
    val jobName = this.getClass().getName() + "-" + System.currentTimeMillis()
    val outputPath = jobName + "_out"
    val inputFile = "src/test/scala/dev/demo/test.txt"
    prepare(jobName, Array(inputFile))

    val sConf = new SparkConf
    sConf.setAppName(jobName)
    val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName()
    sConf.setMaster(masterUrl)
    val sc = new SparkContext(sConf)
    val source = sc.textFile(inputFile)

   try {
      val result = source
        .flatMap(x => x.split(" "))
        .map(x => (x, 1))
        .reduceByKey((x, y) => x + y, 4) 
        .saveAsNewAPIHadoopFile(outputPath, classOf[Text], classOf[IntWritable], classOf[TextOutputFormat[_, _]])
        /*
         * .saveAsTextFile(outputPath)
         * Will not work at the moment since its format is NullWritable/Text which will essentially go into Tez
         * thus creating incompatibility for key/value going into reduce
         */
        
        /*
         * .saveAsHadoopFile(outputPath, classOf[Text], classOf[IntWritable], classOf[org.apache.hadoop.mapred.TextOutputFormat[_,_]])
         * Does work but produces no results. Possibly Tez issue similar to the one I had before with SequenceFileFormat
         */
    } catch {
      // this is temporary. need to figure out how to avoid Spark's writer commit logic since it is the one that throws the exception 
      // Tez already commits the outut for us so we simply need to avoid it. 
      case e: Exception => println(e)
    }
    sc.stop
    
    this.printSampleResults(outputPath)
  }
  
  /**
   * 
   */
  def printSampleResults(outputPath:String) {
    val conf = new TezConfiguration
    val fs = FileSystem.get(conf);
    val iter = fs.listFiles(new Path(outputPath), false);
    var counter = 0;
    var run = true
    while (iter.hasNext() && run) {
      val status = iter.next();
      if (status.isFile()) {
        if (!status.getPath().toString().endsWith("_SUCCESS")) {
          println("Results from " + status.getPath())
          val reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())))
          var line:String = null
          var r = true
          while (r && counter < 20){
            line = reader.readLine()
            if (line == null){
              r = false
            }
            else {
              println(line)
              counter += 1
            }
          }
        }
      }
    }
  }
}