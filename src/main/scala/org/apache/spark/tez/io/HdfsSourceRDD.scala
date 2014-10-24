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

import org.apache.spark.SparkEnv
import org.apache.spark.InterruptibleIterator
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.Partition
import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.FileNotFoundException
import org.apache.tez.dag.api.TezConfiguration
import scala.reflect.ClassTag

/**
 * Replacement for HadoopRDD.
 * Overrides 'compute' methods to be compatible with Tez readers.
 */
abstract class HdfsSourceRDD[T:ClassTag](
  path: String,
  sc: SparkContext,
  @transient conf: Configuration,
  val inputFormatClass: Class[_])
  extends RDD[T](sc, Nil)
  with Logging {

  @transient private val validatedPath = this.validatePath(path)
  this.name = path

  /**
   * 
   */
  override def toString = "name:" + this.name + "; path:" + this.validatedPath

  /**
   * 
   */
  def getPath(): Path = {
    this.validatedPath
  }
  
  /**
   *
   */
  override def getPartitions: Array[Partition] = {
    Array(new HdfsSourceRDDPartition(0))
  }
  
  /**
   *
   */
  private def validatePath(path: String): Path = {
    val fs = FileSystem.get(conf)
    val hPath = new Path(path)
    logInfo("Creating instance of TezRDD for path: " + hPath)
    if (!fs.exists(hPath)) {
      throw new FileNotFoundException("Path: " + hPath + " does not exist")
    }
    hPath
  }
}

private[spark] class HdfsSourceRDDPartition(idx: Int) extends Partition {
  val index = idx
}