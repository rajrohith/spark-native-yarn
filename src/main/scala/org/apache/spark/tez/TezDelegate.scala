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
import org.apache.spark.tez.utils.HadoopUtils;
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

import java.lang.Boolean

import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.mapred.SequenceFileOutputFormat

import java.net.URL

import org.apache.spark.SparkHadoopWriter

import java.net.URI

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.hadoop.security.UserGroupInformation


/**
 *
 */
private[tez] class TezDelegate extends SparkListener with Logging {

  private[tez] val tezConfiguration = new TezConfiguration

  private[tez] var tezClient: Option[TezClient] = None

  private var localResources = new java.util.HashMap[String, LocalResource]

  /**
   *
   */
  def submitApplication[T, U: ClassTag](returnType:ClassTag[U], stage: Stage, func: (TaskContext, Iterator[T]) => U):String = {
    val sc = stage.rdd.context
    logInfo("Job: " + sc.appName + " will be submitted to the following YARN cluster: ")
    this.logYARNConfiguration(this.tezConfiguration)

    val outputMetadata = this.extractOutputMetedata(sc.hadoopConfiguration, sc.appName)
    val tezUtils = new Utils(stage, func, this.localResources)

    if (this.tezClient.isEmpty) {
      this.createLocalResources(sc.appName)
      this.initializeTezClient(sc.appName)
      this.tezClient.get.addAppMasterLocalFiles(localResources);
      this.tezClient.get.start()
    }
    val dagTask: DAGTask = tezUtils.build(returnType, outputMetadata._1, outputMetadata._2, outputMetadata._3, outputMetadata._4)
    dagTask.execute(this.tezClient.get)
    outputMetadata._4
  }

  /**
   * Called when the application ends
   */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    logInfo("Stopping Application: " + applicationEnd)
    if (this.tezClient.isDefined) {
      logInfo("Stopping TezClient: " + this.tezClient.get.getClientName())
      val tezClient = this.tezClient.get
      tezClient.stop()
      val fs = FileSystem.get(tezConfiguration)

      var path = fs.makeQualified(new Path(this.tezClient.get.getClientName + "/tasks"))
      logDebug("Removed: " + path + " - " + fs.delete(path, true))
      path = fs.makeQualified(new Path(this.tezClient.get.getClientName + "/cache"))
      logDebug("Removed: " + path + " - " + fs.delete(path, true))
      path = fs.makeQualified(new Path(this.tezClient.get.getClientName + "/broadcast"))
      logDebug("Removed: " + path + " - " + fs.delete(path, true))
    }
  }

  /**
   * Will create, initialize and start TezClient while also creating a map of LocalResources
   * to be used in application classpath management
   */
  private[tez] def initializeTezClient(appName: String) {
    this.tezClient = new Some(TezClient.create(appName, new TezConfiguration))
  }

  /**
   *
   */
  private def createLocalResources(appName: String){
    val fs = FileSystem.get(tezConfiguration)
    val classpathDir = new Path(appName + "/" + TezConstants.CLASSPATH_PATH)
    val appClassPathDir = fs.makeQualified(classpathDir)
    logInfo("Application classpath dir is: " + appClassPathDir)
    val ucpProp = System.getProperty(TezConstants.UPDATE_CLASSPATH)
    val updateClassPath = ucpProp != null && Boolean.parseBoolean(ucpProp)
    if (updateClassPath) {
      logInfo("Refreshing application classpath, by deleting the existing one. New one will be provisioned")
      fs.delete(appClassPathDir, true)
    }
    val lr = HadoopUtils.createLocalResources(fs, appName + "/" + TezConstants.CLASSPATH_PATH)
    this.localResources.putAll(lr)
  }

  /**
   *
   */
  private def extractOutputMetedata[T, U](conf: Configuration, appName: String): Tuple4[Class[_ <:Writable], Class[_ <:Writable], Class[_], String] = {
    val outputFormat = conf.getClass("mapreduce.job.outputformat.class", classOf[SequenceFileOutputFormat[_, _]])
    val keyType = conf.getClass("mapreduce.job.output.key.class", classOf[KeyWritable], classOf[Writable])
    val valueType = conf.getClass("mapreduce.job.output.value.class", classOf[ValueWritable[_]], classOf[Writable])
    var outputPath = conf.get("mapred.output.dir", "out")

    conf.clear()
    if (!new URI(outputPath).isAbsolute()) {
      outputPath = appName + "/" + outputPath
    }
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
