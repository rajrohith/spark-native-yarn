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

import org.junit.Test
import org.mockito.Mockito._
import org.apache.tez.runtime.api.LogicalOutput
import org.apache.tez.runtime.api.LogicalInput
import java.util.Map
import org.apache.spark.tez.test.utils.TestLogicalOutput
import org.junit.Assert._
import org.junit.runner.RunWith

/**
 * 
 */
class TezShuffleManagerTests {
  
  @Test
  def validateReaderReturned() {
    val (inMap, outMap) = this.createInOutInputMaps
  
    val sm = new TezShuffleManager(inMap, outMap);
    sm.shuffleStage = false
    assertTrue(sm.getReader(null, 0, 0, null).isInstanceOf[TezShuffleReader[_,_]])
  }

  @Test
  def validateAppropriateWriterReturned() {
    val (inMap, outMap) = this.createInOutInputMaps
    
    val smForResultTask = new TezShuffleManager(inMap, outMap);
    smForResultTask.shuffleStage = false
    assertTrue(smForResultTask.getWriter(null, 0, null).isInstanceOf[TezResultWriter[_,_,_]])
    
    val smForShuffleTask = new TezShuffleManager(inMap, outMap);
    assertTrue(smForShuffleTask.getWriter(null, 0, null).isInstanceOf[TezShuffleWriter[_,_,_]])
  }
  
  @Test
  def validateRegisterShuffleReturnsNull() {
    val (inMap, outMap) = this.createInOutInputMaps
    val sm = new TezShuffleManager(inMap, outMap);
    sm.shuffleStage = false
    assertNull(sm.registerShuffle(0,0,null))
  }
  
  @Test
  def validateUnregisterShuffleReturnsTrue() {
    val (inMap, outMap) = this.createInOutInputMaps
    val sm = new TezShuffleManager(inMap, outMap)
    sm.shuffleStage = false
    assertTrue(sm.unregisterShuffle(0))
  }
  
  @Test
  def validateShuffleBlockManagerReturnsNull() {
    val (inMap, outMap) = this.createInOutInputMaps
    val sm = new TezShuffleManager(inMap, outMap)
    sm.shuffleStage = false
    assertNull(sm.shuffleBlockManager)
  }
  
  @Test
  def validateStop() {
    val (inMap, outMap) = this.createInOutInputMaps
    val sm = new TezShuffleManager(inMap, outMap)
    sm.shuffleStage = false
    // asserting that completes successfully even though nothing is done
    sm.stop
  }
  
  private def createInOutInputMaps():Tuple2[java.util.HashMap[Integer, LogicalInput], java.util.HashMap[Integer, LogicalOutput]] = {
    val inMap = new java.util.HashMap[Integer, LogicalInput]()
    inMap.put(0, mock(classOf[LogicalInput]))
    val outMap = new java.util.HashMap[Integer, LogicalOutput]()
    outMap.put(1, new TestLogicalOutput)
    (inMap, outMap)
  }
  
}