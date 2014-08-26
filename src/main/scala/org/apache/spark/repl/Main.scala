///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.spark.repl
//
//import scala.collection.mutable.Set
//import java.io.BufferedReader
//import scala.collection.mutable.ArrayBuffer
//import java.io.StringWriter
//import java.net.URLClassLoader
//import java.io.File
//import java.io.StringReader
//import org.apache.spark.SparkContext
//import org.apache.spark.Tez
//import com.hortonworks.spark.tez.utils.JarUtils
//import java.io.FileInputStream
//import org.apache.spark.SparkUtils
//import java.io.ObjectInputStream
//import org.apache.spark.SparkConf
//
//object Main extends App {
//  private var _interp: TezILoop = _
//
//  def interp = _interp
//
//  def interp_=(i: TezILoop) { _interp = i }
//
//  println("Starting Spark on Tez interactive console")
//  val cl = getClass.getClassLoader
//  var paths = new ArrayBuffer[String]
//  val buffer = new StringBuffer
//  if (cl.isInstanceOf[URLClassLoader]) {
//    val urlLoader = cl.asInstanceOf[URLClassLoader]
//
//    for (url <- urlLoader.getURLs) {
//      if (url.getProtocol == "file") {
//        buffer.append(url.getFile + ":")
//      }
//    }
//  }
//  _interp = new TezILoop
//  _interp.process(Array("-classpath", buffer.toString))
//
//}
//
//class TezILoop extends SparkILoop {
//  override def prompt = "spark-on-tez> "
//  addThunk {
//    intp.beQuietDuring {
//      intp.getInterpreterClassLoader.setAsContext
//      //      println(intp.outputDir)
//      intp.addImports("org.apache.hadoop.fs.FileSystem")
//      intp.addImports("org.apache.hadoop.fs.Path")
//      intp.addImports("org.apache.hadoop.yarn.conf.YarnConfiguration")
//      intp.addImports("org.apache.spark.SparkContext")
//      intp.addImports("org.apache.spark.SparkContext._")
//      intp.addImports("org.apache.spark.Tez")
//    }
//  }
//
//  override def createSparkContext(): SparkContext = {
//    new SparkContext("local", "SparkOnTez-REPL") with Tez
//  }
//
//  override def printWelcome() {
//    echo(this.doWelcome)
//  }
//
//  private def doWelcome() = {
//    "\n" +
//      "                           \\,,,/\n" +
//      "                           (o o)\n" +
//      "                  -----oOOo-(_)-oOOo-----\n" +
//      "  _____                  _                    _______     \n" +
//      " / ____|                | |                  |__   __|      \n" +
//      "| (___  _ __   __ _ _ __| | __   ___  _ __      | | ___ ____\n" +
//      " \\___ \\| '_ \\ / _` | '__| |/ /  / _ \\| '_ \\     | |/ _ \\_  /\n" +
//      " ____) | |_) | (_| | |  |   <  | (_) | | | |    | |  __// / \n" +
//      "|_____/| .__/ \\__,_|_|  |_|\\_\\  \\___/|_| |_|    |_|\\___/___|\n" +
//      "       | |                                                  \n" +
//      "       |_|                                                   \n" +
//      "The following imports are added for your convinince:\n" +
//      "import org.apache.hadoop.fs.FileSystem\n" +
//      "import org.apache.hadoop.fs.Path\n" +
//      "import org.apache.hadoop.yarn.conf.YarnConfiguration\n" +
//      "import org.apache.spark.SparkContext\n" +
//      "import org.apache.spark.SparkContext._\n" +
//      "import org.apache.spark.Tez"
//  }
//}