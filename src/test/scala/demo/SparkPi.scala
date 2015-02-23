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

package demo;

import scala.math.random
import org.apache.spark._
import org.apache.spark.tez.TezJobExecutionContext
import org.apache.spark.tez.TezConstants

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    val conf = buildSparkConf().setAppName("Spark Pi");
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    
    val source = spark.parallelize(1 until n, slices)
    
    val count = source.map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
  
  def buildSparkConf(masterUrl:String = "execution-context:" + classOf[TezJobExecutionContext].getName): SparkConf = {
    System.setProperty(TezConstants.GENERATE_JAR, "true")
    System.setProperty(TezConstants.UPDATE_CLASSPATH, "true")
    
    val sparkConf = new SparkConf
    sparkConf.set("spark.ui.enabled", "false")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    
    sparkConf.setMaster(masterUrl)
    sparkConf
  }
}
