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
import org.apache.spark.tez.io.TezShuffleManager
import com.google.common.base.Preconditions
import org.apache.spark.Partitioner
import org.apache.spark.tez.io.SparkDelegatingPartitioner
import java.net.URLClassLoader
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.tez.dag.api.TezConfiguration
import org.apache.spark.SparkEnv
import java.util.Collections
import akka.util.Collections
import java.util.Collections
import java.util.Comparator
import org.apache.commons.lang.StringUtils

/**
 * Universal Tez processor which aside from providing access to Tez's native readers and writers will also create
 * and instance of custom Spark ShuffleManager thus exposing Tez's reader/writers to Spark.
 * It also contains Spark's serialized tasks (Shuffle and/or Result) which upon de-serialization will be executed
 * essentially providing a delegation model from Tez to Spark native tasks.
 */
class SparkTaskProcessor(context: ProcessorContext) extends SimpleMRProcessor(context) with Logging {
  Preconditions.checkState(context != null, "'context' must not be null".asInstanceOf[Object])
  
  private val dagName = context.getDAGName()
  private val taskIndex = context.getTaskIndex()
  private val vertexName = context.getTaskVertexName()
  
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
    } finally {
      SparkEnv.get.cacheManager.asInstanceOf[TezCacheManager].close
    }
  }

  /**
   * 
   */
  private def doRun() = {
    logInfo("Executing processor for task: " + taskIndex + " for DAG " + dagName);
    val in = this.getInputs()
    val out = this.getOutputs()
  
    val inputs = this.toIntKey(in).asInstanceOf[java.util.Map[Integer, LogicalInput]]
    val outputs = this.toIntKey(out).asInstanceOf[java.util.Map[Integer, LogicalOutput]]
    
    val shufleManager = new TezShuffleManager(inputs, outputs)
    val applicationName = this.context.getDAGName().split("_")(0)
    SparkUtils.createSparkEnv(shufleManager, applicationName)
    
    val registry = this.context.getObjectRegistry()
    
    var task = registry.get(vertexName).asInstanceOf[TezTask[_]]
    if (task == null) {
      val fs = FileSystem.get(new TezConfiguration)
      task = TezHelper.deserializeTask(context, fs)
      registry.cacheForDAG(vertexName, task)
      SparkDelegatingPartitioner.setSparkPartitioner(task.partitioner)
    } 
    
    shufleManager.shuffleStage = task.isInstanceOf[VertexShuffleTask]
    SparkUtils.runTask(task, this.context.getTaskIndex());
    logDebug("Finished task: " + this.context.getTaskVertexName() + " - " + this.context.getTaskIndex())
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
        /*
         * Non-numeric input identifiers contain shuffleId encoding (e.g., 0_1 where 0 is vertex name and 1 is shuffleId)
         * For such cases TezShuffleManager will use shuffleId to link TezShuffleReader to the right input
         */
        if (!StringUtils.isNumeric(indexString)) {
          val shuffleId = Integer.parseInt(indexString.split("_")(1))
          resultMap.put(shuffleId, map.get(indexString));
        } else {
          resultMap.put(Integer.parseInt(indexString), map.get(indexString));
        }
      } catch {
        case e: NumberFormatException =>
          throw new IllegalArgumentException("Vertex name must be parsable to Integer. Was: '" + indexString + "'", e);
      }
    }
    resultMap
  }
}