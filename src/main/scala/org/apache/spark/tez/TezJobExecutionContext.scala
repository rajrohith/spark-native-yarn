package org.apache.spark.tez

import org.apache.spark.JobExecutionContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, Job => NewHadoopJob}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => NewFileOutputFormat}
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.TaskContext
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.scheduler.Stage
import org.apache.spark.ShuffleDependency
import org.apache.spark.util.CallSite
import org.apache.spark.SparkHadoopWriter
import org.apache.spark.SerializableWritable
import org.apache.spark.Logging
import org.apache.spark.tez.io.TezRDD
import org.apache.spark.tez.adapter.SparkToTezAdapter
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.tez.io.ValueWritable
import org.apache.spark.tez.io.ValueWritable


class TezJobExecutionContext extends JobExecutionContext with Logging {
  SparkToTezAdapter.adapt
 /**
  * 
  */
  def hadoopFile[K, V](
    sc:SparkContext,
    path: String,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int = 1): RDD[(K, V)] = {
    
    val fs = FileSystem.get(sc.hadoopConfiguration);
    val qualifiedPath = fs.makeQualified(new Path(path)).toString()
    
    new TezRDD(path, sc, inputFormatClass, keyClass, valueClass, sc.hadoopConfiguration)
  }

  /**
   *
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
    sc: SparkContext,
    path: String,
    fClass: Class[F],
    kClass: Class[K],
    vClass: Class[V],
    conf: Configuration = new Configuration): RDD[(K, V)] = {

    val fs = FileSystem.get(sc.hadoopConfiguration);
    val qualifiedPath = fs.makeQualified(new Path(path)).toString()

    new TezRDD(path, sc, fClass, kClass, vClass, conf)
  }

  /**
   * 
   */
  def broadcast[T: ClassTag](sc:SparkContext, value: T): Broadcast[T] = {
    null
  }

  /**
   * 
   */
  def runJob[T,U](
    sc:SparkContext,
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    allowLocal: Boolean,
    resultHandler: (Int, U) => Unit)(implicit returnType:ClassTag[U]) = {
    
    val outputMetadata = this.extractOutputMetedata(sc.hadoopConfiguration)
    if (outputMetadata == null){
      throw new IllegalArgumentException("Failed to determine output metadata (KEY/VALUE/OutputFormat type)")
    }
    logInfo("Will save output as " + outputMetadata)
    
    
    val stage = this.caclulateStages(sc, rdd)
    val tezUtils = new Utils(stage, func)

    val dagTask: DAGTask = tezUtils.build(outputMetadata._1, outputMetadata._2, outputMetadata._3, outputMetadata._4)
    dagTask.execute

//    println("doing result")
//    partitions.foreach { x =>
//      println(x)
//      /*
//       * Call some custom function that would figure out value for second argument in 
//       * resultHandler if return type is not Unit
//       */
//      resultHandler(x, 3L.asInstanceOf[U])
//    }
  }
  
  /**
   * 
   */
  private def caclulateStages(sc:SparkContext, rdd:RDD[_]):Stage = {
    val ds = sc.dagScheduler
    val method = ds.getClass.getDeclaredMethod("newStage", classOf[RDD[_]], classOf[Int], classOf[Option[ShuffleDependency[_, _, _]]], classOf[Int], classOf[CallSite])
    method.setAccessible(true)
    val stage = method.invoke(ds, rdd, new Integer(1), None, new Integer(0), org.apache.spark.util.Utils.getCallSite).asInstanceOf[Stage]
    stage
  }
  
  /**
   * 
   */
  private def extractOutputMetedata[T,U](conf:Configuration):Tuple4[Class[_], Class[_], Class[_], String] = {  
    val outputFormat = conf.getClass("mapreduce.job.outputformat.class", null)
    val keyType = conf.getClass("mapreduce.job.output.key.class", null)
    val valueType = conf.getClass("mapreduce.job.output.value.class", Class.forName("org.apache.spark.tez.io.ValueWritable"))
    val outputPath = conf.get("mapred.output.dir", null)
    if (outputPath == null || outputFormat == null || keyType == null){
      null
    }
    else {
      (keyType, valueType, outputFormat, outputPath)
    }
  }
}