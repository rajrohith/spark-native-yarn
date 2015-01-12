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
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.junit.Assert

/**
 * 
 */
class JoinTests {

  @Test
  def joinWithShuffleCoGroups() {
    val file1 = "src/test/scala/org/apache/spark/tez/file1.txt"
    val file2 = "src/test/scala/org/apache/spark/tez/file2.txt"
    val applicationName = "joinWithTwoShuffleCoGroup"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source1 = sc.textFile(file1)
    val source2 = sc.textFile(file2)

    val two = source2.map { x =>
      val s = x.split(" ")
      val key: Int = Integer.parseInt(s(0))
      println("mapping source 2")
      (key, s(1))
    }
    
    val result = source1.map { x =>
      val s = x.split(" ")
      println("mapping source 1")
      val key: Int = Integer.parseInt(s(2))
      val t = (key, (s(0), s(1)))
      t
    }.join(two).reduceByKey { (x, y) => ((x._1.toString, y._1.toString), x._2)
    }.collect

    Assert.assertEquals(3, result.toList.size)
    // ===

    sc.stop
    this.cleanUp(applicationName)
  }
  
  @Test
  def joinWithShuffleAndNarrowCoGroup() {
    val file1 = "src/test/scala/org/apache/spark/tez/file1.txt"
    val file2 = "src/test/scala/org/apache/spark/tez/file2.txt"
    val applicationName = "joinWithShuffleAndNarrowCoGroup"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source1 = sc.textFile(file1)
    val source2 = sc.textFile(file2)

    val two = source2.map { x =>
      val s = x.split(" ")
      val key: Int = Integer.parseInt(s(0))
      println("mapping source 2")
      (key, s(1))
    }.reduceByKey{(x, y) => println("Reducing source 2"); x}
    
    val result = source1.map { x =>
      val s = x.split(" ")
      println("mapping source 1")
      val key: Int = Integer.parseInt(s(2))
      val t = (key, (s(0), s(1)))
      t
    }.join(two).reduceByKey { (x, y) => ((x._1.toString, y._1.toString), x._2)
    }.collect

    Assert.assertEquals(3, result.toList.size)
    // ===

    sc.stop
    this.cleanUp(applicationName)
  }
  
  @Test
  def joinWithNarrowAndShuffleCoGroup() {
    val file1 = "src/test/scala/org/apache/spark/tez/file1.txt"
    val file2 = "src/test/scala/org/apache/spark/tez/file2.txt"
    val applicationName = "joinWithNarrowAndShuffleCoGroup"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source1 = sc.textFile(file1)
    val source2 = sc.textFile(file2)

    val two = source2.map { x =>
      val s = x.split(" ")
      val key: Int = Integer.parseInt(s(0))
      println("mapping source 2")
      (key, s(1))
    }
    
    two.collect
    
    val result = source1.map { x =>
      val s = x.split(" ")
      println("mapping source 1")
      val key: Int = Integer.parseInt(s(2))
      val t = (key, (s(0), s(1)))
      t
    }.reduceByKey{(x, y) => println("Reducing source 1"); x}.join(two).reduceByKey { (x, y) => ((x._1.toString, y._1.toString), x._2)
    }.collect

    Assert.assertEquals(3, result.toList.size)
    // ===

    sc.stop
    this.cleanUp(applicationName)
  }
  
  @Test
  def joinWithNarrowCoGroups() {
    val file1 = "src/test/scala/org/apache/spark/tez/file1.txt"
    val file2 = "src/test/scala/org/apache/spark/tez/file2.txt"
    val applicationName = "joinWithNarrowCoGroups"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source1 = sc.textFile(file1)
    val source2 = sc.textFile(file2)

    val two = source2.map { x =>
      val s = x.split(" ")
      val key: Int = Integer.parseInt(s(0))
      println("mapping source 2")
      (key, s(1))
    }.reduceByKey{(x, y) => println("Reducing source 2"); x}
    
    val result = source1.map { x =>
      val s = x.split(" ")
      println("mapping source 1")
      val key: Int = Integer.parseInt(s(2))
      val t = (key, (s(0), s(1)))
      t
    }.reduceByKey{(x, y) => println("Reducing source 2"); x}.join(two).reduceByKey { (x, y) => ((x._1.toString, y._1.toString), x._2)
    }.collect

    Assert.assertEquals(3, result.toList.size)
    // ===

    sc.stop
    this.cleanUp(applicationName)
  }
  
  
  @Test
  def joinWithTupleAsKey() {
   
    val applicationName = "joinWithTupleAsKey"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source1 = sc.parallelize(List(	((1,4),0.0), ((1,9),1.0), ((1,9),3.456), ((1,3),5.46)))
    val source2 = sc.parallelize(List(((1,9),0.0), ((1,3),0.0)))

    val result = source1.join(source2).collect.toList
    Assert.assertEquals(3, result.size)
    Assert.assertEquals(List(((1,9),(3.456,0.0)), ((1,9),(1.0,0.0)), ((1,3),(5.46,0.0))), result)
    // ===

    sc.stop
    this.cleanUp(applicationName)
  }
  
   /**
   *
   */
  def cleanUp(applicationname: String) {
    FileUtils.deleteDirectory(new File(applicationname))
  }

  /**
   * To execute the same code via Spark, simply pass 'local' as an argument (e.g., buildSparkConf("local"))
   */
  def buildSparkConf(masterUrl:String = "execution-context:" + classOf[TezJobExecutionContext].getName): SparkConf = {
    val sparkConf = new SparkConf
    sparkConf.set("spark.ui.enabled", "false")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    sparkConf.setMaster(masterUrl)
    sparkConf
  }
}