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
import scala.reflect.runtime.universe._
import java.lang.reflect.Field
import org.junit.Assert._
import org.apache.tez.dag.api.TezConfiguration
import org.apache.hadoop.fs.FileSystem
import scala.collection.mutable.Set
import org.apache.hadoop.fs.Path
import org.mockito.Mockito._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.storage.StorageLevel
import org.apache.spark.tez.io.TezRDD
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.spark.tez.utils.ReflectionUtils
import org.mockito.Mockito
import org.mockito.internal.matchers.Any
import org.mockito.Mock
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import java.io.File
import org.junit.runner.RunWith
import org.apache.spark.tez.test.utils.Instrumentable
import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.tez.test.utils.TezClientMocker
import org.apache.tez.client.TezClient
import org.mockito.Matchers
import org.apache.tez.dag.api.DAG
import org.apache.commons.io.FileUtils
import org.apache.spark.tez.test.utils.TestUtils
import org.apache.spark.SparkConf

/**
 *
 */
class TezJobExecutionContextTests extends Instrumentable {

  @Test
  def validateInitialization() {
    val tec = new TezJobExecutionContext
    assertTrue(ReflectionUtils.getFieldValue(tec, "cachedRDDs").isInstanceOf[Set[_]])
    assertTrue(ReflectionUtils.getFieldValue(tec, "runJobInvocationCounter").isInstanceOf[AtomicInteger])
    assertTrue(ReflectionUtils.getFieldValue(tec, "tezDelegate").isInstanceOf[TezDelegate])
    assertTrue(ReflectionUtils.getFieldValue(tec, "fs").isInstanceOf[FileSystem])
  }

//  @Test FIX!
//  def validateInitialPersist() {
//    val appName = "validateInitialPersist"
//    val (sc, rdd, tec, persistedRddName) = this.doPersist(appName)
//    val persistedRdd = tec.persist(sc, rdd, StorageLevel.NONE)
//    val set = ReflectionUtils.getFieldValue(tec, "cachedRDDs").asInstanceOf[Set[Path]]
//    assertEquals(1, set.size)
//
//    assertEquals(persistedRddName, new File(set.iterator.next.toUri()).getName())
//    assertNotNull(persistedRdd)
//    assertTrue(new File(persistedRddName).exists())
//    TestUtils.cleanup(persistedRddName)
//    sc.stop
//  }
//
//  @Test FIX!
//  def validateSubsequentPersist() {
//    val appName = "validateSubsequentPersist"
//    val (sc, rdd, tec, persistedRddName) = this.doPersist(appName)
//    val persistedRdd = tec.persist(sc, rdd, StorageLevel.NONE)
//    tec.persist(sc, persistedRdd, StorageLevel.NONE)
//    assertNotNull(persistedRdd)
//    assertTrue(new File(persistedRddName).exists())
//    TestUtils.cleanup(persistedRddName)
//    sc.stop
//  }
//
//  @Test FIX!
//  def validateUnpersist() {
//    val appName = "validateUnpersist"
//    val (sc, rdd, tec, persistedRddName) = this.doPersist(appName)
//    val persistedRdd = tec.persist(sc, rdd, StorageLevel.NONE)
//    assertTrue(new File(persistedRddName).exists())
//    tec.unpersist(sc, persistedRdd)
//    assertFalse(new File(persistedRddName).exists())
//    // just to ensure that subsequent un-persist will not result in exception
//    tec.unpersist(sc, persistedRdd)
//    TestUtils.cleanup(persistedRddName)
//    sc.stop
//  }

  /**
   * 
   */
  private def doPersist(appName:String): Tuple4[SparkContext, RDD[_], TezJobExecutionContext, String] = {
    val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName
    val sparkConf = new SparkConf
    sparkConf.setAppName(appName)
    sparkConf.setMaster(masterUrl)
    sparkConf.set("spark.ui.enabled", "false")
    val sc = new SparkContext(sparkConf)
    ReflectionUtils.setFieldValue(sc, "executionContext.tezDelegate.tezClient", new Some(TezClientMocker.noOpTezClientWithSuccessfullSubmit(appName)))
    val tec = ReflectionUtils.getFieldValue(sc, "executionContext").asInstanceOf[TezJobExecutionContext]
    val rdd = new TezRDD("src/test/scala/org/apache/spark/tez/sample.txt", sc, classOf[TextInputFormat],
      classOf[Text], classOf[IntWritable],
      (ReflectionUtils.getFieldValue(tec, "tezDelegate.tezConfiguration").asInstanceOf[TezConfiguration]))

    val persistedRddName = TestUtils.stubPersistentFile(appName, rdd)
    tec.persist(sc, rdd, StorageLevel.NONE)

    val tezClient = ReflectionUtils.getFieldValue(sc, "executionContext.tezDelegate.tezClient").asInstanceOf[Option[TezClient]].get
    val inOrder = Mockito.inOrder(tezClient)
    inOrder.verify(tezClient, Mockito.times(1)).waitTillReady()
    inOrder.verify(tezClient, Mockito.times(1)).submitDAG(Matchers.any[DAG])
    (sc, rdd, tec, persistedRddName)
  }
}