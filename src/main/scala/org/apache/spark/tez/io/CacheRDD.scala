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

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.Partition
import scala.reflect.ClassTag
import org.apache.spark.TaskContext
import org.apache.spark.InterruptibleIterator
import org.apache.spark.SparkEnv
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.io.FileNotFoundException

/**
 * 
 */
class CacheRDD[T:ClassTag](sc: SparkContext,
    path: String,
    @transient conf: Configuration) extends RDD[T](sc, Nil){
  
  @transient private val fqPath = this.validatePath(path)
  this.name = path

  override def toString = "name:" + this.name + "; path:" + this.fqPath

  def getPath(): Path = {
    this.fqPath
  }

  override def getPartitions: Array[Partition] = {
    Array(new Partition {
      override def index: Int = 0
    })
  }
  
  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[T] = {
    val iterator = SparkEnv.get.shuffleManager.getReader(null, 0, 0, context).read.asInstanceOf[Iterator[(Any, ValueWritable)]]
    new InterruptibleIterator(context, iterator.map(_._2.getValue().asInstanceOf[Array[T]]).flatMap(_.toIterator))
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