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
package org.apache.spark.tez.test.utils

import org.apache.spark.rdd.RDD
import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.tez.dag.api.TezConfiguration
import org.apache.hadoop.fs.FileSystem
import java.io.BufferedReader
import org.apache.hadoop.fs.Path
import java.io.InputStreamReader
import org.apache.spark.SparkContext
import org.apache.tez.client.TezClient
import org.apache.spark.tez.TezDelegate
import org.apache.spark.tez.utils.ReflectionUtils
import org.mockito.Mockito
/**
 *
 */
object TestUtils {

  /**
   *
   */
  def stubPersistentFile(appName: String, rdd: RDD[_]): String = {
    val cache = new File(appName + "_cache_" + rdd.id)
    cache.createNewFile()
    cache.getName()
  }

  /**
   *
   */
  def cleanup(testName: String) {
    FileUtils.deleteQuietly(new File(testName))
  }

  /**
   * 
   */
  def instrumentTezClient(sc: SparkContext): TezClient = {
    val tezDelegate = ReflectionUtils.getFieldValue(sc, "executionContext.tezDelegate").asInstanceOf[TezDelegate]
    tezDelegate.initializeTezClient(sc.appName)
    val tezClient = TezClient.create(sc.appName, new TezConfiguration)
    tezClient.start()
    val watchedTezClient = Mockito.spy(tezClient)
    ReflectionUtils.setFieldValue(tezDelegate, "tezClient", new Some(watchedTezClient))
    watchedTezClient
  }

  /**
   *
   */
  def printSampleResults(appName:String, outputPath: String) {
    val conf = new TezConfiguration
    val fs = FileSystem.get(conf);
    val iter = fs.listFiles(new Path(appName + "/" + outputPath), false);
    var counter = 0;
    var run = true
    while (iter.hasNext() && run) {
      val status = iter.next();
      if (status.isFile()) {
        if (!status.getPath().toString().endsWith("_SUCCESS")) {
          println("Results from " + status.getPath() + " - " + fs.getLength(status.getPath()))

          val reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())))
          var line: String = null
          var r = true
          while (r && counter < 20) {
            line = reader.readLine()
            if (line == null) {
              r = false
            } else {
              println("\t" + line)
              counter += 1
            }
          }
        }
      }
    }
  }
}