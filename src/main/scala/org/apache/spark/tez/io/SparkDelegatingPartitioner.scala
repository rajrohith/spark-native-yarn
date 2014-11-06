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

import org.apache.tez.runtime.library.partitioner.HashPartitioner
/**
 * A companion object of SparkDelegatingPartitioner which maintains an instance of 
 * the actual Partitioner de-serialized and injected by SparkTaskProcessor
 */
private[tez] object SparkDelegatingPartitioner {
  private var sparkPartitioner:org.apache.spark.Partitioner = _
  
  /**
   * 
   */
  private[tez] def setSparkPartitioner(sparkPartitioner:org.apache.spark.Partitioner) {
    this.sparkPartitioner = sparkPartitioner
  }
}
/**
 * A special implementation of the Partitioner which maintains awareness of an actual 
 * instance of the Partitioner created during Spark's DAG assembly to enable existing Spark's 
 * capabilities of supplying an instance of the Partitioner against Tez's class of the Partitioner.
 * 
 * The awareness is maintained by caching the actual instance in the companion object. The instance will
 * be injected into companion object after its being de-serialized in SparkTaskProcessor.
 */
private[tez] class SparkDelegatingPartitioner extends HashPartitioner {
  /**
   * 
   */
  override def getPartition(key:Object, value:Object, numPartitions:Int):Int = {
    val p = if (SparkDelegatingPartitioner.sparkPartitioner != null){
      SparkDelegatingPartitioner.sparkPartitioner.getPartition(key)
    }
    else {
      super.getPartition(key, value, numPartitions)
    }
    p
  }
}