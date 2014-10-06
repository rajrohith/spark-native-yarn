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
package dev.demo

import java.io.{InputStreamReader, BufferedReader, File}
import java.net.URL
import java.net.URLClassLoader

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.tez.TezConstants
import org.apache.tez.dag.api.TezConfiguration

object DemoUtilities {

  def prepareJob(inputFiles: Array[String]) : String = {
    val jobName = this.getClass().getName()
    val cl = ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader]
    val m = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL]);
    m.setAccessible(true);
    val file = new File("src/test/resources/mini/").toURI().toURL()
    m.invoke(cl, file)

    if (jobName != null && inputFiles != null) {
      System.setProperty(TezConstants.GENERATE_JAR, "true");
      val configuration = new TezConfiguration

      val fs = FileSystem.get(configuration);
      fs.delete(new Path(jobName + "_out"))
      inputFiles.foreach(x => fs.copyFromLocalFile(new Path(x), new Path(x)))
    }
    jobName
  }

  /**
   *
   */
  def printSampleResults(outputPath: String) {
    val conf = new TezConfiguration
    val fs = FileSystem.get(conf);
    val iter = fs.listFiles(new Path(outputPath), false);
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