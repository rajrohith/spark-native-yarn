package org.apache.spark.tez

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.ShuffleDependency
import org.apache.spark.Logging
import org.apache.spark.scheduler.Task
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.tez.runtime.library.api.KeyValueWriter
import org.apache.spark.tez.io.DelegatingWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.TaskContext

/**
 * Tez vertex Task modeled after Spark's ShufleMapTask
 */
class VertexShuffleTask(
    stageId: Int,
    rdd:RDD[_], 
    val dep: Option[ShuffleDependency[Any, Any, Any]]) extends Task[MapStatus](stageId, 0) with Logging {

  override def runTask(context: TaskContext): MapStatus = {
    var writer: ShuffleWriter[Any, Any] = null
    try {
      val manager = SparkEnv.get.shuffleManager
      val partition = null
      val sh = new BaseShuffleHandle(0, 0, dep.get)
      writer = manager.getWriter[Any, Any](sh, 1, new TaskContext(1,1,1))
      
      writer.write(rdd.iterator(partition, null).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])

      return writer.stop(success = true).get
    } catch {
      case e: Exception =>
        if (writer != null) {
          writer.stop(success = false)
        }
        throw e
    } 
  }
}