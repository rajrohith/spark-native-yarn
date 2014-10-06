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

import org.apache.spark.Logging
import org.apache.tez.mapreduce.processor.SimpleMRProcessor
import org.apache.tez.runtime.api.LogicalInput
import org.apache.tez.runtime.api.LogicalOutput
import org.apache.tez.runtime.api.ProcessorContext
import java.io.File
import org.apache.tez.runtime.library.processor.SimpleProcessor
import org.apache.spark.tez.io.TezShuffleManager
import org.apache.spark.scheduler.Task
import java.io.ByteArrayInputStream
import org.apache.spark.tez.io.TypeAwareStreams.TypeAwareObjectInputStream

/**
 * 
 */
object SparkTaskProcessor {
  var task:Task[_] = null
  var vertexIndex = -1
}
/**
 * Universal Tez processor which aside from providing access to Tez's native readers and writers will also create
 * and instance of custom Spark ShuffleManager thus exposing Tez's reader/writers to Spark.
 * It also contains Spark's serialized tasks (Shuffle and/or Result) which upon deserialization will be executed
 * essentially providing a delegation model from Tez to Spark native tasks.
 */
class SparkTaskProcessor(val context: ProcessorContext) extends SimpleMRProcessor(context) with Logging {
  
  /**
   * 
   */
  def run() = {
    try {
      this.doRun();
    } catch {
      case e: Exception =>
        e.printStackTrace();
        throw new IllegalStateException("Failed to execute processor for Vertex " + this.getContext().getTaskVertexIndex(), e);
    }
  }

  /**
   * 
   */
  private def doRun() = {
    logInfo("Executing processor for task: " + this.getContext().getTaskIndex() + " for DAG " + this.getContext().getDAGName());
    val inputs = this.toIntKey(this.getInputs()).asInstanceOf[java.util.Map[Integer, LogicalInput]]
    val outputs = this.toIntKey(this.getOutputs()).asInstanceOf[java.util.Map[Integer, LogicalOutput]]
  
    if (SparkTaskProcessor.task == null) {
      val taskBytes = TezUtils.getTaskBuffer(context)      
      val bis = new ByteArrayInputStream(taskBytes)
      val is = new TypeAwareObjectInputStream(bis)
      SparkTaskProcessor.task = is.readObject().asInstanceOf[Task[_]]
      SparkTaskProcessor.vertexIndex = context.getTaskVertexIndex()
    } else if (context.getTaskVertexName() != SparkTaskProcessor.vertexIndex){
      val taskBytes = TezUtils.getTaskBuffer(context)
      val bis = new ByteArrayInputStream(taskBytes)
      val is = new TypeAwareObjectInputStream(bis)
      SparkTaskProcessor.task = is.readObject().asInstanceOf[Task[_]]
      SparkTaskProcessor.vertexIndex = context.getTaskVertexIndex()
    } 
    
    val shufleManager = 
      if (SparkTaskProcessor.task.isInstanceOf[VertexResultTask[_,_]]){
        val vrt = SparkTaskProcessor.task.asInstanceOf[VertexResultTask[_,_]]
        new TezShuffleManager(inputs, outputs, false);
      } else {
        new TezShuffleManager(inputs, outputs);
      }

    SparkUtils.createSparkEnv(shufleManager);
    SparkUtils.runTask(SparkTaskProcessor.task);
  }

  /**
   * 
   */
  private def toIntKey(map: java.util.Map[String, _]): java.util.Map[Integer, _] = {
    val resultMap = new java.util.TreeMap[Integer, Any]();
    val iter = map.keySet().iterator()
    while (iter.hasNext()) {
      val indexString = iter.next()
      try {
        resultMap.put(Integer.parseInt(indexString), map.get(indexString));
      } catch {
        case e: NumberFormatException =>
          throw new IllegalArgumentException("Vertex name must be parsable to Integer. Was: '" + indexString + "'", e);
      }
    }
    resultMap
  }
}