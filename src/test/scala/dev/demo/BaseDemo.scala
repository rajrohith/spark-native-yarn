package dev.demo

import java.net.URLClassLoader
import java.net.URL
import java.io.File
import org.apache.spark.tez.instrument.TezInstrumentationAgent
import org.apache.spark.tez.TezConstants
import org.apache.tez.dag.api.TezConfiguration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapred.SequenceFileRecordReader
import org.apache.spark.SparkEnv
import java.nio.ByteBuffer

trait BaseDemo {

  def prepare(jobName:String, inputFiles:Array[String]) {
    val cl = ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader]
    val m = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL]);
	m.setAccessible(true);
	val file = new File("src/test/resources/mini/").toURI().toURL()
	m.invoke(cl, file)
    
    System.setProperty(TezConstants.GENERATE_JAR, "true");
    val configuration = new TezConfiguration
    
    val fs = FileSystem.get(configuration);
    fs.delete(new Path(jobName + "_out"))
    inputFiles.foreach(x => fs.copyFromLocalFile(new Path(x), new Path(x)))
    
  }
}