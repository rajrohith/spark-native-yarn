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

import java.io.Serializable
import scala.reflect.ClassTag
import org.apache.spark.broadcast.Broadcast
import java.util.UUID
import org.apache.tez.dag.api.TezConfiguration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URL
/**
 * 
 */
class TezBroadcast[T: ClassTag](@transient var broadcastedValue: T, applicationName:String) extends Broadcast[T](0L) {
  
  val bid = UUID.randomUUID().toString()
  
  var path:String = null;
  
  this.saveToHdfs

  override protected def getValue() = {
    if (this.broadcastedValue == null){
      val bValis = new URL(path).openStream()
      this.broadcastedValue = SparkUtils.deserialize(bValis).asInstanceOf[T]
    }
    this.broadcastedValue
  }
  
  def saveToHdfs(){
    val config = new TezConfiguration
    val fs = FileSystem.get(config)
    val serValueBuffer = SparkUtils.serialize(broadcastedValue);
    val file = ClassPathUtils.ser(serValueBuffer, applicationName + "_broadcast_" + bid + ".ser");
    file.deleteOnExit()
    path = YarnUtils.provisionResource(file, fs, applicationName).toString()
    println(path)
  }

  override protected def doUnpersist(blocking: Boolean) {/*noop*/}

  override protected def doDestroy(blocking: Boolean) {/*noop*/}
}