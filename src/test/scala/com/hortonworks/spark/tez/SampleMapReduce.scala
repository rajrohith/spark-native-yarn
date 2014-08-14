package com.hortonworks.spark.tez

import scala.reflect.ClassTag
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.DAGExecutor
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.tez.TezDelegatingExecutor
import org.apache.spark.tez.rdd.ParallelCollectionRDD_override
import com.hortonworks.spark.tez.utils.ClassLoaderUtils
import org.apache.spark.tez.TezDelegatingExecutor
import org.apache.spark.tez.rdd.SparkContext_override

object SampleMapReduce extends App {
  
  val conf = new YarnConfiguration
  val fs = FileSystem.get(conf);
  
//  
//  val file = new Path("foo.seq")
//  val writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(file), SequenceFile.Writer.keyClass(classOf[IntWritable]), SequenceFile.Writer.valueClass(classOf[Text]))
//  writer.append(new IntWritable(0), new Text("Hello"))
//  writer.append(new IntWritable(1), new Text("Nastia"))
//  writer.append(new IntWritable(2), new Text("Oleg"))
//  writer.append(new IntWritable(3), new Text("Seva"))
//  
//  writer.close()
//  
//  val reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file))
//  val key = new IntWritable
//  while (reader.next(key)){
//    if (key.get() == 2){
//      val text = new Text
//      reader.getCurrentValue(text)
//      println(text)
//    }
//  }
  
//  var testFile = new Path("data/sample-256.txt");
//  fs.copyFromLocalFile(false, true, new Path("/Users/ozhurakousky/dev/spark-on-tez/sample-256.txt"), testFile);
//  testFile = new Path("data/file1.txt");
//  fs.copyFromLocalFile(false, true, new Path("/Users/ozhurakousky/dev/spark-on-tez/file1.txt"), testFile);
//  testFile = new Path("data/file2.txt");
//  fs.copyFromLocalFile(false, true, new Path("/Users/ozhurakousky/dev/spark-on-tez/file2.txt"), testFile);
//   ClassLoaderUtils.loadClassFromBytesAs(classOf[ParallelCollectionRDD_override[_]], "org.apache.spark.rdd.ParallelCollectionRDD");
  ClassLoaderUtils.loadClassAs(classOf[ParallelCollectionRDD_override[_]], "org.apache.spark.rdd.ParallelCollectionRDD");
  ClassLoaderUtils.loadClassAs(classOf[SparkContext_override], "org.apache.spark.SparkContext");
 
  val sc = new SparkContext("external:" + classOf[TezDelegatingExecutor].getName, "SparkOnTez-WordCount") 
//  val sc = new SparkContext("local", "SparkOnTez-WordCount") 
  
  println("##### STARTING JOB");
  
//  val source = sc.textFile("/user/ozhurakousky/data")
  val source = sc.parallelize(List("hello my friend", "hello my buddy", "foo bar", "hello"), 4)
//  val result = source
//    .flatMap ( line => line.split(" ") )
//    .map((_, 1L))
//    .reduceByKey(_ + _)
//    .collect
//
//    println("######## FINISHED")
//  println(result.toSet)
  
  
  sc.stop
}