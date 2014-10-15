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

import org.apache.spark.TaskContext
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.scheduler.Stage
import org.apache.tez.client.TezClient
import org.apache.tez.dag.api.TezConfiguration
import scala.reflect.ClassTag
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.Logging
import org.apache.hadoop.io.Writable
import org.apache.spark.tez.io.KeyWritable
import org.apache.spark.tez.io.ValueWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import java.lang.Boolean
import org.apache.hadoop.yarn.api.records.LocalResource

/**
 * 
 */
class TezDelegate extends SparkListener with Logging {

  private[tez] val tezConfiguration = new TezConfiguration

  private var tezClient: Option[TezClient] = None
  
  private var localResources:java.util.Map[String, LocalResource] = null

  /**
   *
   */
  def submitApplication[T, U: ClassTag](appName: String, configuration: Configuration, stage: Stage, func: (TaskContext, Iterator[T]) => U) {
    logInfo("Job: " + appName + " will be submitted to the following YARN cluster: ")
    this.logYARNConfiguration(this.tezConfiguration)
    if (this.tezClient.isEmpty) {
      this.initializeAndStartTezClient(appName)
    }
    val tezUtils = if (this.localResources == null) {
      new Utils(this.tezClient.get, stage, func)
    } else {
      new Utils(this.tezClient.get, stage, func, localResources)
    }
    val outputMetadata = this.extractOutputMetedata(configuration, appName)
    val dagTask: DAGTask = tezUtils.build(outputMetadata._1, outputMetadata._2, outputMetadata._3, outputMetadata._4)
    dagTask.execute
  }

  /**
   * Called when the application ends
   */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    if (this.tezClient.isDefined) {
      logInfo("Stopping TezClient")
      val tezClient = this.tezClient.get
      tezClient.stop()
    }
  }

  /**
   * Will create, initialize and start TezClient while also creating a map of LocalResources
   * to be used in application classpath management
   */
  private[tez] def initializeAndStartTezClient(appName: String) {
    val fs = FileSystem.get(tezConfiguration);
    this.tezClient = new Some(TezClient.create(appName, new TezConfiguration))
    val classpathDir = new Path(this.tezClient.get.getClientName() + "/" + TezConstants.CLASSPATH_PATH)
    val appClassPathDir = fs.makeQualified(classpathDir)
    logInfo("Application classpath dir is: " + appClassPathDir)
    val ucpProp = System.getProperty(TezConstants.UPDATE_CLASSPATH)

    val updateClassPath = ucpProp != null && Boolean.parseBoolean(ucpProp)
    if (updateClassPath) {
      logInfo("Refreshing application classpath, by deleting the existing one. New one will be provisioned")
      fs.delete(appClassPathDir)
    }
    this.localResources = YarnUtils.createLocalResources(fs, appName + "/" + TezConstants.CLASSPATH_PATH)
    this.tezClient.get.addAppMasterLocalFiles(localResources);

    this.tezClient.get.start()
  }

  /**
   *
   */
  private def extractOutputMetedata[T, U](conf: Configuration, appName: String): Tuple4[Class[_ <:Writable], Class[_ <:Writable], Class[_], String] = {
    val outputFormat = conf.getClass("mapreduce.job.outputformat.class", classOf[TextOutputFormat[_, _]])
    val keyType = conf.getClass("mapreduce.job.output.key.class", classOf[KeyWritable], classOf[Writable])
    val valueType = conf.getClass("mapreduce.job.output.value.class", classOf[ValueWritable], classOf[Writable])
    val outputPath = conf.get("mapred.output.dir", appName + "_out")
    conf.clear()
    if (outputPath == null || outputFormat == null || keyType == null) {
      throw new IllegalArgumentException("Failed to determine output metadata (KEY/VALUE/OutputFormat type)")
    } else {
      logDebug("Will save output as \nkey:" + keyType + "; \nvalue:" + valueType +
        "; \noutputFormat:" + outputFormat + "; \noutputPath: " + outputPath)
      (keyType, valueType, outputFormat, outputPath)
    }
  }

  /**
   *
   */
  private def logYARNConfiguration(configuration: Configuration) {
    logInfo("Default FS Address: " + configuration.get("fs.defaultFS"))
    logInfo("RM Host Name: " + configuration.get("yarn.resourcemanager.hostname"))
    logInfo("RM Address: " + configuration.get("yarn.resourcemanager.address"))
    logInfo("RM Scheduler Address: " + configuration.get("yarn.resourcemanager.scheduler.address"))
    logInfo("RM Resource Tracker Address: " + configuration.get("yarn.resourcemanager.resourcetracker.address"))
  }
}