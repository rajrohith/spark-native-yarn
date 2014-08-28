package org.apache.spark

import java.nio.ByteBuffer
import org.apache.spark.scheduler.ResultTask
import org.apache.spark.scheduler.Task
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.BlockManager
import org.apache.spark.scheduler.ResultTask
import sun.misc.Unsafe
import org.apache.spark.rdd.CoGroupPartition
import java.io.InputStream
import java.io.ByteArrayInputStream
import sun.reflect.ReflectionFactory
import java.io.ObjectInputStream
import org.apache.spark.tez.VertexTask
import org.apache.spark.tez.VertexTask

/**
 * Utility functions related to Spark functionality.
 * Mainly used by TezSparkProcessor to deserialize tasks and 
 * create light version of SparkEnv to satisfy Spark requirements 
 * (e.g., avoid NPE mainly)
 */
object SparkUtils {
  val sparkConf = new SparkConf
  val unsafeConstructor = classOf[Unsafe].getDeclaredConstructor();
  unsafeConstructor.setAccessible(true);
  val unsafe = unsafeConstructor.newInstance();

  /**
   * 
   */
  def createUnsafeInstance[T](clazz:Class[T]):T = {
    unsafe.allocateInstance(clazz).asInstanceOf[T]
  }

  /**
   * 
   */
  def createSparkEnv(shuffleManager:TezShuffleManager) {
    val blockManager = unsafe.allocateInstance(classOf[BlockManager]).asInstanceOf[BlockManager];   
    val ser = new JavaSerializer(sparkConf)
    val se = new SparkEnv("0", null, ser, ser, null, null, shuffleManager, null, blockManager, null, null, null, null, null, null, sparkConf)
    SparkEnv.set(se)
  }

  /**
   * 
   */
  def deserializeSparkTask(taskBytes: Array[Byte], partitionId:Int): VertexTask = {
    val serializer = SparkEnv.get.serializer.newInstance
    val taskBytesBuffer = ByteBuffer.wrap(taskBytes)
    val task = serializer.deserialize[VertexTask](taskBytesBuffer)

    task
  }
  
  /**
   * 
   */
  def runTask(task: VertexTask) = { 
    task.runTask
  }
}