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
  
  def getLastMethodName():String = {
    val ex = new Exception
    ex.getStackTrace().filter(_.toString().contains("org.apache.spark.rdd")).last.getMethodName()
  }
  
  /**
   * 
   */
  def serialize(value:Any):ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    val os = new TypeAwareObjectOutputStream(bos)
    os.writeObject(value)
    ByteBuffer.wrap(bos.toByteArray())
  }
  
  /**
   * 
   */
  def deserialize(ois:InputStream):Object = {
    val is = new TypeAwareObjectInputStream(ois)
    is.readObject()
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
  def createSparkEnv(shuffleManager:TezShuffleManager) {
    val tm = new TaskMetrics
    TaskContext.setTaskContext(new TaskContext(1, 1, 1, true, tm))
    val blockManager = unsafe.allocateInstance(classOf[BlockManager]).asInstanceOf[BlockManager];   
    val memoryManager = new ShuffleMemoryManager(20793262)
    val se = new SparkEnv("0", null, closueSerializer, closueSerializer, null, null, shuffleManager, null, null, blockManager, null, null, null, null, memoryManager, sparkConf)
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