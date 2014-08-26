package org.apache.spark.tez

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkEnv
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.TaskContext
import org.apache.spark.TaskContext
import scala.reflect.ClassTag
import org.apache.spark.Partition

class VertexResultTask[T,U](rdd:RDD[T], func: (TaskContext, Iterator[T]) => U) extends VertexTask(rdd, null) {
  
  override def runTask(): Any = {
    // Deserialize the RDD and the func using the broadcast variables.
//    val ser = SparkEnv.get.closureSerializer.newInstance()
//    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
//      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

//    metrics = Some(context.taskMetrics)
      val partition:Partition = new DummyPartition
      val context = new TaskContext(1,1,1)
    try {
      val result = func(context, rdd.iterator(partition, context))
//      println(result.asInstanceOf[Array[_]].toList)
    } finally {
//      context.markTaskCompleted()
    }
//    val result = Array.empty[Any].asInstanceOf[U]
//    result
//    ()
  }
  
  private class DummyPartition extends Partition {
    def index: Int = 1
  }

}