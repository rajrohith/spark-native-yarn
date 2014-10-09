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
import org.apache.spark.tez.test.utils.ReflectionUtils
import org.apache.spark.tez.test.utils.StarkTest
import org.mockito.Mockito
import org.mockito.internal.matchers.Any
import org.mockito.Mock
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import java.io.File
/**
 * 
 */
class TezJobExecutionContextTests extends StarkTest {
  
  @Test
  def validateInitialization(){
    val tec = new TezJobExecutionContext
    assertTrue(ReflectionUtils.getFieldValue(tec, "tezConfig").isInstanceOf[TezConfiguration])
    assertTrue(ReflectionUtils.getFieldValue(tec, "fs").isInstanceOf[FileSystem])
    assertTrue(ReflectionUtils.getFieldValue(tec, "cachedRDDs").isInstanceOf[Set[_]])
  }
  
  @Test
  def validateInitialPersist(){
    val (sc, rdd, tec, persistedRddName) = this.doPersist
    val persistedRdd = tec.persist(sc, rdd, StorageLevel.NONE)
    val set = ReflectionUtils.getFieldValue(tec, "cachedRDDs").asInstanceOf[Set[Path]]
    assertEquals(1, set.size)
    assertEquals(new File(persistedRddName).toURI().toString(), set.iterator.next.toUri().toString())
    assertNotNull(persistedRdd)
    assertTrue(new File(persistedRddName).exists())
  }
  
  @Test
  def validateSubsequentPersist(){
    val (sc, rdd, tec, persistedRddName) = this.doPersist
    val persistedRdd = tec.persist(sc, rdd, StorageLevel.NONE)
    tec.persist(sc, persistedRdd, StorageLevel.NONE)
    assertNotNull(persistedRdd)
    assertTrue(new File(persistedRddName).exists())
  }
  
   @Test
  def validateUnpersist(){
    val (sc, rdd, tec, persistedRddName) = this.doPersist
    val persistedRdd = tec.persist(sc, rdd, StorageLevel.NONE)
    assertTrue(new File(persistedRddName).exists())
    tec.unpersist(sc, persistedRdd)
    assertFalse(new File(persistedRddName).exists())
    // just to ensure that subsequent un-persist will not result in exception
    tec.unpersist(sc, persistedRdd)
  }
  
  private def doPersist():Tuple4[SparkContext, RDD[_], TezJobExecutionContext, String] = {
    val masterUrl = "execution-context:" + classOf[TezJobExecutionContext].getName
    val sc = new SparkContext(masterUrl, "validatePersist")
    val tec = ReflectionUtils.getFieldValue(sc, "executionContext").asInstanceOf[TezJobExecutionContext]
    val rdd = new TezRDD("src/test/scala/org/apache/spark/tez/sample.txt", sc, classOf[TextInputFormat], 
        classOf[Text], classOf[IntWritable], 
        (ReflectionUtils.getFieldValue(tec, "tezConfig").asInstanceOf[TezConfiguration]))

    val persistedRddName = "validatePersist_cache_" + rdd.id
    this.stubPersistentFile(persistedRddName)
    (sc, rdd, tec, persistedRddName)
  }

  private def stubPersistentFile(persistedRddName:String) {
    val file = new File(persistedRddName)
    file.createNewFile()
    file.deleteOnExit()
  }
}