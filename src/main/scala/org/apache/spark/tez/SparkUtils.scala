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
  
  val taskContext = new TaskContext(1,1,1)

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
    val memoryManager = new ShuffleMemoryManager(20793262)
    val se = new SparkEnv("0", null, closueSerializer, closueSerializer, null, null, shuffleManager, null, null, blockManager, null, null, null, null, memoryManager, sparkConf)
    SparkEnv.set(se)
  }

  /**
   * 
   */
  def deserializeSparkTask(taskBytes: Array[Byte], partitionId:Int): Task[_] = {
    val taskBytesBuffer = ByteBuffer.wrap(taskBytes)
    val task = closureSerializerInstance.deserialize[Task[_]](taskBytesBuffer)
    task
  }
  
  /**
   * 
   */
  def runTask(task: Task[_]) = { 
    task.runTask(taskContext)
  }
}