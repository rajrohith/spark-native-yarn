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

import org.apache.tez.runtime.library.api.KeyValueReader
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.tez.io.TypeAwareStreams.TypeAwareObjectInputStream
import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.Logging
import java.io.Closeable
import java.io.EOFException

/**
 * 
 */
class CacheReader(path:Path, fs:FileSystem) extends KeyValueReader with Logging with Closeable{
  private val is = new TypeAwareObjectInputStream(fs.open(path))
  private val itemCounter = new AtomicInteger
  private var keepReading = true
  private var obj:Object = _

  def next(): Boolean = {
    try {
      obj = is.readObject()
      obj
    } catch {
      case e: EOFException =>
        keepReading = false;
    }
    keepReading
  }

 /**
  * 
  */
  override def getCurrentKey():Object = {
    itemCounter.get().asInstanceOf[Integer]
  }
  
  /**
   * 
   */
  override def getCurrentValue():Object = {
    logTrace("Cache read: " + obj)
    itemCounter.getAndIncrement()
    obj
  }
  
  /**
   * 
   */
  override def close() {
    if (is != null){
      is.close()
    }
  }
}