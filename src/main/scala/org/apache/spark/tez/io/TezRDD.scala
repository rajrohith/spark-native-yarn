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

/**
 * Replacement for HadoopRDD.
 * Overrides 'compute' methods to be compatible with Tez readers.
 */
class TezRDD[K, V](
  path: String,
  sc: SparkContext,
  val inputFormatClass: Class[_],
  val keyClass: Class[K],
  val valueClass: Class[V],
  @transient conf: Configuration)
  extends RDD[(K, V)](sc, Nil)
  with Logging {

  @transient private val fqPath = this.validatePath(path)
  this.name = path

  override def toString = "name:" + this.name + "; path:" + this.fqPath

  def getPath(): Path = {
    this.fqPath
  }

  /**
   *
   */
  override def getPartitions: Array[Partition] = {
    Array(new Partition {
      override def index: Int = 0
    })
  }
  /**
   *
   */
  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iterator = SparkEnv.get.shuffleManager.getReader(null, 0, 0, null).read.asInstanceOf[Iterator[(K, V)]]
    new InterruptibleIterator(context, iterator)
  }
  /**
   *
   */
  private def validatePath(path: String): Path = {
    val fs = FileSystem.get(conf)
    val qPath = fs.makeQualified(new Path(path))
    logInfo("Creating instance of TezRDD for path: " + qPath)
    if (!fs.exists(qPath)) {
      throw new FileNotFoundException("Path: " + qPath + " does not exist")
    }
    qPath
  }
}