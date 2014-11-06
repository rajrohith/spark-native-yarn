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
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import breeze.linalg.{ Vector, DenseVector, squaredDistance }
import org.apache.hadoop.io.NullWritable
import org.apache.spark.tez.io.KeyWritable
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileUtil
import java.io.File
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.HashPartitioner
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.junit.Assert

/**
 * Will run in Tez local mode
 */

class MllibDemoTests extends Serializable {

  @Test
  def als() {
    FileUtils.deleteDirectory(new File("als"))
    val applicationName = "als"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    
    val implicitPrefs = true

    val training = sc.textFile("src/test/scala/org/apache/spark/tez/sample_movielens_data.txt").map { line =>
      val fields = line.split("::")
      if (implicitPrefs) {
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - 2.5)
      } else {
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
      }
    }
    
    val model = new ALS()
      .setRank(2)
      .setIterations(1)
      .setLambda(0.3)
      .setImplicitPrefs(true)
      .setUserBlocks(2)
      .setProductBlocks(3)
      .run(training)

    Assert.assertEquals(100, model.productFeatures.collect.toList.size)
    Assert.assertEquals(2, model.rank)
    Assert.assertEquals(30, model.userFeatures.collect.toList.size)
   
    sc.stop()
    this.cleanUp(applicationName)
  }

  def buildSparkConf(masterUrl:String = "execution-context:" + classOf[TezJobExecutionContext].getName): SparkConf = {
    val sparkConf = new SparkConf
    sparkConf.set("spark.ui.enabled", "false")
    sparkConf.setMaster(masterUrl)
    sparkConf
  }

  /**
   *
   */
  def cleanUp(applicationname: String) {
    FileUtils.deleteDirectory(new File(applicationname))
  }
}