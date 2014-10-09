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
package org.apache.spark.tez.io

import org.apache.spark.tez.test.utils.StarkTest
import org.junit.Test
import org.mockito.Mockito._
import org.apache.tez.runtime.api.LogicalOutput
import org.apache.tez.runtime.api.LogicalInput
import java.util.Map
import org.apache.spark.tez.test.utils.TestLogicalOutput
import org.junit.Assert._
import org.apache.spark.tez.test.utils.TestLogicalInput
import java.io.File
import org.apache.spark.tez.test.utils.TestLogicalInputWithKVsReader

/**
 * 
 */
class TezShuffleReaderTests extends StarkTest {
  
  @Test
  def validateKVReaderReturned() {
    val inMap = new java.util.HashMap[Integer, LogicalInput]()
    val uri = new File("src/test/scala/org/apache/spark/tez/io/tezRDDTestFile.txt").toURI
    inMap.put(0, new TestLogicalInput(uri))
    val reader = new TezShuffleReader[Integer, String](inMap)
    val iter = reader.read
    assertEquals(3, iter.toList.size)
  }
  
  @Test
  def validateKVSReaderReturned() {
    val inMap = new java.util.HashMap[Integer, LogicalInput]()
    
    inMap.put(0, new TestLogicalInputWithKVsReader())
    val reader = new TezShuffleReader[Integer, String](inMap)
    val iter = reader.read
    assertEquals(2, iter.toList.size)
  }
  
  @Test
  def validateStop() {
    val inMap = new java.util.HashMap[Integer, LogicalInput]()
    val uri = new File("src/test/scala/org/apache/spark/tez/io/tezRDDTestFile.txt").toURI
    inMap.put(0, new TestLogicalInput(uri))
    val reader = new TezShuffleReader[Integer, String](inMap)
    // ensure does not result in exception
    reader.stop
  }

}