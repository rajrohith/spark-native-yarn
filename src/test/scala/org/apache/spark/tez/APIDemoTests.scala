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
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.SparkConf
import org.junit.Assert
import org.apache.commons.io.FileUtils
import java.io.File
import org.apache.spark.tez.test.utils.TestUtils
import org.apache.spark.HashPartitioner
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable
import org.mockito.Mockito
import org.mockito.Matchers
import java.util.concurrent.atomic.AtomicInteger
import java.io.Serializable
import java.io.DataOutputStream
import java.io.FileOutputStream
import java.io.DataInputStream
import java.io.FileInputStream
import scala.io.Source
import java.nio.ByteBuffer
import java.io.InputStream

/**
 * Will run in Tez local mode
 */
class APIDemoTests {

  @Test
  def collect() {
    val applicationName = "collect"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source = sc.textFile("src/test/scala/org/apache/spark/tez/sample.txt")

    // ===
    val result = source
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .filter(_._2 > 3)
      .collect
    // ===
    Assert.assertEquals(26, result.length)
    sc.stop
    this.cleanUp(applicationName)
  }
  
  @Test
  def sample() {
    val applicationName = "sample"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source = sc.textFile("src/test/scala/org/apache/spark/tez/sample.txt")

    // ===
    val result = source.sample(true, 0.2, 2L).collect
    Assert.assertTrue(result.length > 0)
    // ===
    
    sc.stop
    this.cleanUp(applicationName)
  }
 
  @Test
  def mapPartitions() {
  
    val applicationName = "mapPartitions"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source = sc.textFile("src/test/scala/org/apache/spark/tez/sample.txt", 4)

    // ===
    // greater then contents-in-file
    val gtResult = source.mapPartitions{_.grouped(10).map{_.toArray}}.collect
    gtResult.foreach{x => println("==="); x.foreach(println _)}
    Assert.assertEquals(28, gtResult.flatten.length)
    
    // less then contents-in-file
    val ltResult = source.mapPartitions{_.grouped(2).map{_.toArray}}.collect
    ltResult.foreach{x => println("==="); x.foreach(println _)}
    Assert.assertEquals(28, ltResult.flatten.length)
    
     // pre-cached
    val cacheResult = source.cache.mapPartitions{_.grouped(10).map{_.toArray}}.collect
    cacheResult.foreach{x => println("==="); x.foreach(println _)}
    Assert.assertEquals(28, cacheResult.flatten.length)
    // ===
    
    sc.stop
    this.cleanUp(applicationName)
  }
  
  @Test
  def mapPartitionsWithIndex() {
    val applicationName = "mapPartitionsWithIndex"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source = sc.textFile("sample-data/mapPartitionWithIndex")
    // ===
    // greater then contents-in-file
    val grouped = source.map(x => (x, 1)).partitionBy(new HashPartitioner(4))
    .mapPartitionsWithIndex { (blockId, elements) => {
        println("BLOCKID: " + blockId)
        val f = new File(applicationName + "/invocations")
        val os = new DataOutputStream(new FileOutputStream(f, true))
        os.writeInt(blockId)
        os.close()
        elements
      }
    }.saveAsNewAPIHadoopFile("foo", classOf[Text],
      classOf[IntWritable], classOf[TextOutputFormat[_, _]])
   
    val is = new DataInputStream(new FileInputStream(new File(applicationName + "/invocations")))
    var counter = 0
    Iterator.continually(is.available()).takeWhile(x => x != 0).foreach{_ => is.readInt; counter += 1}
    is.close()
    Assert.assertEquals(4, counter)
    // ===
//    println("RESULT: " + gtResult.toList)
    sc.stop
    this.cleanUp(applicationName)
  }
  
  def inputStreamToByteArray(is: InputStream): Array[Byte] =
  Iterator continually is.read takeWhile (-1 !=) map (_.toByte) toArray
  
  @Test
  def saveAsTextFile() {
    val applicationName = "saveAsTextFile"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source = sc.textFile("src/test/scala/org/apache/spark/tez/sample.txt")

    // ===
    val result = source
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .saveAsTextFile(applicationName + "/" + applicationName + "_out")
    // ===

    TestUtils.printSampleResults(applicationName, applicationName + "_out")
    sc.stop
    this.cleanUp(applicationName)
  }
  
  @Test
  def reduceByKey() {
    val applicationName = "reduceByKey"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source = sc.textFile("sample-data/reduceBy")

    // ===
    val result = source
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_+_)
      .cache
      
      println(result.collect.toList)

      result.saveAsNewAPIHadoopFile(applicationName + "_out", classOf[Text],
        classOf[IntWritable], classOf[TextOutputFormat[_, _]])

    // ===
    TestUtils.printSampleResults(applicationName, applicationName + "_out")
    sc.stop
    this.cleanUp(applicationName)
  }
  
  @Test
  def mapValues() {
    val applicationName = "mapValues"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source = sc.textFile("src/test/scala/org/apache/spark/tez/sample.txt")

    // ===
    val r = source
      .flatMap(x => x.split(" "))
      .map(x => (x, 1)).mapValues(_ + 1).collect
    
    println(r.toList)

    sc.stop
    this.cleanUp(applicationName)
  }

  @Test
  def count() {
    val applicationName = "count"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source = sc.textFile("src/test/scala/org/apache/spark/tez/sample.txt")

    // ===
    val result = source
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .count
    // ===  

    Assert.assertEquals(292, result)
    sc.stop
    this.cleanUp(applicationName)
  }
  
  @Test
  def sourceCount() {
    val applicationName = "sourceCount"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source = sc.textFile("src/test/scala/org/apache/spark/tez/sample.txt")

    // ===
    val result = source.count     
    // ===  
    Assert.assertEquals(28, result)
    sc.stop
    this.cleanUp(applicationName)
  }

  @Test
  def join() {
    val file1 = "src/test/scala/org/apache/spark/tez/file1.txt"
    val file2 = "src/test/scala/org/apache/spark/tez/file2.txt"
    val applicationName = "join"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source1 = sc.textFile(file1)
    val source2 = sc.textFile(file2)

    /*
     * The issue is that now with ResultWriter we recognize types.
     * But in this case we have two different types for value - String(1) and Tuple2(2)
     * So need to think if we jsut save as ValueWritable
     */
    // ===
    val two = source2.distinct.map { x =>
      val s = x.split(" ")
      val key: Int = Integer.parseInt(s(0))
      (key, s(1))
    }.cache
    
    println(two.collect.toList)
    
    val result = source1.map { x =>
      val s = x.split(" ")
      val key: Int = Integer.parseInt(s(2))
      val t = (key, (s(0), s(1)))
      t
    }.join(two).reduceByKey { (x, y) => ((x._1.toString, y._1.toString), x._2)
    }.collect
    
    println(result.toList)
    // ===

    sc.stop
    this.cleanUp(applicationName)
  }
  
  
 

  @Test
  def partitionBy() {
    val applicationName = "partitionBy"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source = sc.textFile("src/test/scala/org/apache/spark/tez/partitioning.txt")

    // since partitioner will be serialized even in Tez local mode
    // file is created as an evidence that the method was invoked
    val partitioner = new HashPartitioner(2) {
      override def getPartition(key: Any): Int = {
        val f = new File(applicationName + "_executed")
        f.createNewFile()
        f.deleteOnExit()
        super.getPartition(key)
      }
    }

    // ===
    val result = source
      .map { s => val split = s.split("\\s+", 2); (split(0).replace(":", "_"), split(1)) }
      .partitionBy(partitioner)
      .saveAsHadoopFile(applicationName +  "/" + applicationName + "_out",
        classOf[Text], classOf[Text], classOf[KeyPerPartitionOutputFormat])
    // ===

    Assert.assertTrue(new File(applicationName + "_executed").exists())
    TestUtils.printSampleResults(applicationName, applicationName + "_out")
    sc.stop
    this.cleanUp(applicationName)
  }

  @Test
  def collectPartitions() {
    val applicationName = "collectPartitions"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source = sc.textFile("src/test/scala/org/apache/spark/tez/sample.txt")

    // ===
    val result = source
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(new HashPartitioner(3), (x, y) => x + y)
      .collectPartitions
    // ===

    Assert.assertEquals(3, result.length)
    sc.stop
    this.cleanUp(applicationName)
  }

  @Test
  def cache() {
    val applicationName = "cache"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source = sc.textFile("src/test/scala/org/apache/spark/tez/sample.txt")

    val ts = System.currentTimeMillis()
    // ===
    val rdbRDD = source
       // emulating long running computation
      .flatMap{x => println("###### COMPUTING"); Thread.sleep(50); x.split(" ")}
      .map(x => (x, 1))
      .reduceByKey{(x, y) => 
        println("###### REDUCING");
        val f = new File(applicationName + ts) // marker file to validate that function was invoked
        f.createNewFile()
        f.deleteOnExit()
        x + y
      }
      
    val result = rdbRDD.cache
      
    // ===
    Assert.assertEquals(292, result.count) // should take much longer then the next call
    Assert.assertTrue(new File(applicationName + ts).exists())
    new File(applicationName + ts).delete()
    Assert.assertFalse(new File(applicationName + ts).exists())
    // you should see no subsequent recompute 
    Assert.assertEquals(292, result.count)
    Assert.assertFalse("Recompute happened even though RDD was cached", new File(applicationName + ts).exists())

    sc.stop
    this.cleanUp(applicationName)
  }

  @Test
  def parallelize() {
    val applicationName = "parallelize"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)
    val source = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 0), 4)
    val count = source.filter(_ % 2 == 0).count
    Assert.assertEquals(5, count)
    this.cleanUp(applicationName)
  }

  @Test
  def broadcast() {
    val applicationName = "broadcast"
    this.cleanUp(applicationName)
    val sparkConf = this.buildSparkConf()
    sparkConf.setAppName(applicationName)
    val sc = new SparkContext(sparkConf)

    val list = List(1, 3, 5)
    val bList = sc.broadcast[List[Int]](list)

    val source = sc.parallelize(List(1, 2, 4, 5, 6, 7, 8, 9, 0), 1)
    val count = source.filter(bList.value.contains(_)).count
    Assert.assertEquals(2, count)
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
    sparkConf.setMaster(masterUrl)
    sparkConf
  }
}

/**
 *
 */
class KeyPerPartitionOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any = {
    NullWritable.get()
  }

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    key.toString
  }
}

class Department(val id:Integer, val name:String) extends Serializable {
  override def toString() = {
    "ID:" + id + "; DEPT:" + name 
  }
}