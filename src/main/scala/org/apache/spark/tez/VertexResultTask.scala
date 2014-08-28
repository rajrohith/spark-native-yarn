package org.apache.spark.tez

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkEnv
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.TaskContext
import org.apache.spark.TaskContext
import scala.reflect.ClassTag
import org.apache.spark.Partition

/**
 * Tez vertex Task modeled after Spark's ResultTask
 */
class VertexResultTask[T, U](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U) extends VertexTask(rdd, null) {

  /**
   * 
   */
  override def runTask(): Any = {
    val partition: Partition = new DummyPartition
    val context = new TaskContext(1, 1, 1)
    val result = func(context, rdd.iterator(partition, context))
  }

  /**
   * 
   */
  private class DummyPartition extends Partition {
    def index: Int = 1
  }
}