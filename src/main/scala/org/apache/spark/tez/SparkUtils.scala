/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.tez

import java.nio.ByteBuffer
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.BlockManager
import sun.misc.Unsafe
import org.apache.spark.SparkConf
import org.apache.spark.SparkEnv
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.BlockManager
import sun.misc.Unsafe
import org.apache.spark.TaskContext
import org.apache.spark.scheduler.Task
import org.apache.spark.shuffle.ShuffleMemoryManager
import org.apache.spark.tez.io.TezShuffleManager
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.tez.io.TypeAwareStreams.TypeAwareObjectOutputStream
import org.apache.spark.tez.io.TypeAwareStreams.TypeAwareObjectInputStream
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.io.InputStream
import org.apache.spark.rdd.RDD
import java.io.OutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.CacheManager
import org.apache.spark.Partition
import org.apache.spark.storage.StorageLevel
import org.apache.spark.tez.io.ValueWritable
import org.apache.hadoop.io.NullWritable
import org.apache.spark.ShuffleDependency
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.tez.dag.api.TezConfiguration
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import scala.io.Source
import java.io.FileOutputStream
import tachyon.client.OutStream
import scala.collection.mutable.HashSet
import org.apache.spark.tez.io.CacheReader

/**
 * Utility functions related to Spark functionality.
 * Mainly used by TezSparkProcessor to deserialize tasks and 
 * create light version of SparkEnv to satisfy Spark requirements 
 * (e.g., avoid NPE mainly)
 */
object SparkUtils {
  val sparkConf = new SparkConf
  val closueSerializer = new JavaSerializer(sparkConf)
  val closureSerializerInstance = closueSerializer.newInstance
  val unsafeConstructor = classOf[Unsafe].getDeclaredConstructor();
  unsafeConstructor.setAccessible(true);
  val unsafe = unsafeConstructor.newInstance();
  
  /**
   * 
   */
  def getLastMethodName():String = {
    val ex = new Exception
    ex.getStackTrace().filter(_.toString().contains("org.apache.spark.rdd")).head.getMethodName()
  }
  
  /**
   * 
   */
  def serializeToBuffer(value:Any):ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    this.serializeToOutputStream(value, bos)
    ByteBuffer.wrap(bos.toByteArray())
  }
  
  /**
   * 
   */
  def serializeToOutputStream(value:Any, outputStream:OutputStream) {
    val os = new TypeAwareObjectOutputStream(outputStream)
    os.writeObject(value)
  }
  
  /**
   * 
   */
  def serializeToFs(value:Any, fs:FileSystem, path:Path):Path = {
    val os = fs.create(path)
    SparkUtils.serializeToOutputStream(value, os)
    os.close()
    if (fs.exists(path)){
      path
    } else {
      throw new IllegalStateException("Failed to serialize: " + path  + " to: " + fs)
    }
  }
  
  /**
   * 
   */
  def deserialize(ois:InputStream):Object = {
    val is = new TypeAwareObjectInputStream(ois)
    val result = is.readObject()
    is.close
    result
  }

  /**
   * 
   */
  def createUnsafeInstance[T](clazz:Class[T]):T = {
    unsafe.allocateInstance(clazz).asInstanceOf[T]
  }

  /**
   * 
   */
  def createSparkEnv(shuffleManager:TezShuffleManager, applicationName:String) {
    val blockManager = unsafe.allocateInstance(classOf[BlockManager]).asInstanceOf[BlockManager];  
    val cacheManager = new TezCacheManager(blockManager, applicationName)
    val memoryManager = new ShuffleMemoryManager(20793262)
    val se = new SparkEnv("0", null, closueSerializer, closueSerializer, cacheManager, null, shuffleManager, 
        null, blockManager, null, null, null, null, null, memoryManager, sparkConf)
    SparkEnv.set(se)
  }
  
  /**
   * 
   */
  def runTask(task: Task[_], taskIndex:Int) = { 
    val taskContext = new TaskContext(0, taskIndex, 0)
    task.runTask(taskContext)
  }
}
/**
 *
 */
private[tez] class TezCacheManager(blockManager: BlockManager, applicationName: String) extends CacheManager(blockManager) {
  var is: ObjectInputStream = null

  val outStreamSet = new HashSet[OutputStream]

  override def getOrCompute[T](
    rdd: RDD[T],
    partition: Partition,
    context: TaskContext,
    storageLevel: StorageLevel): Iterator[T] = {

    val fs = FileSystem.get(new TezConfiguration)
    val path = new Path(applicationName + "/cache/cache_" + rdd.id + "/part-" + context.partitionId)
    if (fs.exists(path)) {
      logDebug("Reading " + rdd + " from cache: " + path)
      is = new TypeAwareObjectInputStream(fs.open(path))
      new Iterator[T] {
        val cr = new CacheReader(path, fs)
        def hasNext(): Boolean = {
          cr.next
        }
        def next(): T = {
          cr.getCurrentValue.asInstanceOf[T]
        }
      }
    } else {
      val os = new TypeAwareObjectOutputStream(fs.create(path))
      outStreamSet.add(os)
      rdd.computeOrReadCheckpoint(partition, context).map { obj =>
        os.writeObject(obj)
        logTrace("Caching: '" + obj + "' of " + rdd + " in " + path)
        obj
      }
    }
  }

  def close() {
    outStreamSet.foreach(_.close())
    outStreamSet.clear
    if (is != null) {
      is.close()
    }
  }
}