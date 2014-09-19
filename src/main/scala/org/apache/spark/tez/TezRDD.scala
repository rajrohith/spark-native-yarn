package org.apache.spark.tez

import org.apache.spark.SparkEnv
import org.apache.spark.InterruptibleIterator
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.Partition
import org.apache.spark.Logging
import org.apache.spark.SparkContext

/**
 * Replacement for HadoopRDD.
 * Overrides 'compute' methods to be compatible with Tez readers.
 */
class TezRDD[K, V](
  path: String,
  sc: SparkContext,
  val inputFormatClass: Class[_],
  val keyClass: Class[K],
  val valueClass: Class[V],
  @transient conf: Configuration)
  extends RDD[(K, V)](sc, Nil)
  with Logging {

  this.name = path

  logInfo("Creating instance of TezRDD for path: " + path)

  override def toString = this.name
  
  def getPath():String = {
    this.name
  }

  /**
   *
   */
  override def getPartitions: Array[Partition] = {
    Array(new Partition {
      override def index: Int = 0
    })
  }
  /**
   *
   */
  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iterator = SparkEnv.get.shuffleManager.getReader(null, 0, 0, null).read.asInstanceOf[Iterator[(K, V)]]
    new InterruptibleIterator(new TaskContext(0, 1, 1, true), iterator)
  }
}
