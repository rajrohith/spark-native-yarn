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

import org.junit.Test
import org.junit.Assert
import org.mockito.Mockito
import org.apache.tez.runtime.api.ProcessorContext
import org.apache.spark.tez.utils.ReflectionUtils
import org.apache.tez.runtime.api.LogicalInput
import org.apache.tez.runtime.api.LogicalOutput
import org.apache.spark.tez.test.utils.TestLogicalInput
import java.net.URI
import java.io.File
import org.apache.tez.dag.api.UserPayload
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.tez.client.TezClient
import org.apache.spark.SparkConf
import org.apache.tez.dag.api.TezConfiguration
import org.mockito.Matchers
import org.apache.tez.dag.api.DAG
import org.apache.hadoop.yarn.api.records.LocalResource

/**
 *
 */
class SparkTaskProcessorTests {

  @Test
  def validateNullContextFailure() {
    try {
      new SparkTaskProcessor(null)
      Assert.fail
    } catch {
      case e: IllegalArgumentException => // success
      case other: Exception => Assert.fail
    }
  }

  @Test
  def fullCycleWithTezClientOrderValidation() {

    val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName
    val applicationName = "reduceByKey"
    val sparkConf = new SparkConf
    sparkConf.set("spark.ui.enabled", "false")
    sparkConf.setAppName(applicationName);
    sparkConf.setMaster(masterUrl)
    val sc = new SparkContext(sparkConf)

    val tezClient = this.instrumentTezClient(sc, applicationName)

    val source = sc.textFile("src/test/scala/org/apache/spark/tez/sample.txt")
    val result = source
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .saveAsNewAPIHadoopFile("out", classOf[Text],
        classOf[IntWritable], classOf[TextOutputFormat[_, _]])
    sc.stop
    
    val inOrder = Mockito.inOrder(tezClient)
    inOrder.verify(tezClient, Mockito.times(1)).addAppMasterLocalFiles(Matchers.any[java.util.Map[String, LocalResource]])
    inOrder.verify(tezClient, Mockito.times(1)).waitTillReady()
    inOrder.verify(tezClient, Mockito.times(1)).submitDAG(Matchers.any[DAG])
    inOrder.verify(tezClient, Mockito.times(1)).stop
  }

  private def instrumentTezClient(sc: SparkContext, applicationName: String):TezClient = {
    val tezDelegate = ReflectionUtils.getFieldValue(sc, "executionContext.tezDelegate").asInstanceOf[TezDelegate]
    val tezClient = TezClient.create(applicationName, new TezConfiguration)
    tezClient.start()
    val watchedTezClient = Mockito.spy(tezClient)
    ReflectionUtils.setFieldValue(tezDelegate, "tezClient", new Some(watchedTezClient))
    watchedTezClient
  }
}