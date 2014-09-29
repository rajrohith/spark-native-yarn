package org.apache.spark.tez.adapter

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.TaskContext
import org.apache.hadoop.io.Writable
import org.apache.spark.Logging
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.Partitioner
import org.apache.spark.rdd.ShuffledRDD

/**
 *
 */
class PairRDDFunctionsAdapter[K, V] extends Logging {

  def saveAsNewAPIHadoopDataset(conf: Configuration) {
    val fields = this.getClass().getDeclaredFields().filter(_.getName().endsWith("self"))
    val field = fields(0)
    field.setAccessible(true)
    val self: RDD[_] = field.get(this).asInstanceOf[RDD[_]]

    val outputFormat = conf.getClass("mapreduce.job.outputformat.class", null)
    self.context.hadoopConfiguration.setClass("mapreduce.job.outputformat.class", outputFormat, classOf[org.apache.hadoop.mapreduce.OutputFormat[_, _]])

    val keyType = conf.getClass("mapreduce.job.output.key.class", null)
    self.context.hadoopConfiguration.setClass("mapreduce.job.output.key.class", keyType, classOf[Writable])

    val valueType = conf.getClass("mapreduce.job.output.value.class", null)
    self.context.hadoopConfiguration.setClass("mapreduce.job.output.value.class", valueType, classOf[Writable])

    val outputPath = conf.get("mapred.output.dir")
    self.context.hadoopConfiguration.set("mapred.output.dir", outputPath)

    self.context.runJob(self, (context: TaskContext, iter: Iterator[_]) => ())
  }

  def saveAsHadoopDataset(conf: JobConf) {
    val fields = this.getClass().getDeclaredFields().filter(_.getName().endsWith("self"))
    val field = fields(0)
    field.setAccessible(true)
    val self: RDD[_] = field.get(this).asInstanceOf[RDD[_]]

    val outputFormat = conf.getClass("mapred.output.format.class", null)
    self.context.hadoopConfiguration.setClass("mapreduce.job.outputformat.class", outputFormat, classOf[org.apache.hadoop.mapred.OutputFormat[_, _]])

    val keyType = conf.getClass("mapreduce.job.output.key.class", null)
    self.context.hadoopConfiguration.setClass("mapreduce.job.output.key.class", keyType, classOf[Writable])

    val valueType = conf.getClass("mapreduce.job.output.value.class", null)
    self.context.hadoopConfiguration.setClass("mapreduce.job.output.value.class", valueType, classOf[Writable])

    val outputPath = conf.get("mapred.output.dir")
    self.context.hadoopConfiguration.set("mapred.output.dir", outputPath)

    self.context.runJob(self, (context: TaskContext, iter: Iterator[_]) => ())
  }

  def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] = {
    val fields = this.getClass().getDeclaredFields().filter(_.getName().endsWith("self"))
    val field = fields(0)
    field.setAccessible(true)
    val self: RDD[Product2[K,V]] = field.get(this).asInstanceOf[RDD[Product2[K,V]]]

    val rdd = new ShuffledRDD[K, V, V](self, partitioner)
    rdd.asInstanceOf[RDD[(K, Iterable[V])]]
  }
}