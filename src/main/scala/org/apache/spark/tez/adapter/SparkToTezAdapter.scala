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
package org.apache.spark.tez.adapter

import javassist.ClassPool
import javassist.CtClass
import javassist.CtMethod
import sun.misc.Unsafe
import java.util.HashMap
import javassist.ClassMap
import javassist.Modifier
import javassist.CtField
import org.apache.tez.dag.api.TezConfiguration
import java.net.URLClassLoader
import javassist.ClassPath
import javassist.LoaderClassPath
import org.apache.spark.Logging
import org.apache.spark.rdd.CoGroupedRDD

/**
 * Byte-code instrumentation agent based on Javassist API - http://www.csg.ci.i.u-tokyo.ac.jp/
 * Its main purpose is to instrument various Spark classes in order to allow Spark to support
 * Tez execution engine.
 * It allows type-safe instrumentation based on swapping re-implemented methods, constructors and fields
 * with your own defined in some other class, as long as their definitions match; essentially causing the same effect
 * as if the target class was extended.
 *
 * For example:
 * class Printer {
 *    public void print(){
 *    	System.out.println("Hi")
 *    }
 * }
 *
 * class MyPrinter {
 *    public void print(){
 *      System.out.println(MyPrinter.getClass().getName())
 *    	System.out.println("Bye")
 *    }
 * }
 *
 * This class can swap 'print' method in the Printer class with the one in MyPrinter
 * since method definitions are the same, making Printer look like MyPrinter without
 * it being extended.
 *
 * In case if there are references inside the methods to the class that is used as source of transformation,
 * those references will also be replaced with the target class. So the print method of MyPrinter class will print
 * Printer instead of MyPrinter
 *
 */
object SparkToTezAdapter extends Logging {

  private var klass:Class[_] = null
  /**
   *
   */
  def adapt = {
    this.synchronized {
      if (klass == null) {
        this.doAdapt
      }
    }
  }

  private def doAdapt {
    val systemClassLoader = Thread.currentThread().getContextClassLoader()
    val pool = ClassPool.getDefault();
    

    val pairRddFunctionsAdapter = pool.get("org.apache.spark.tez.adapter.PairRDDFunctionsAdapter")
    val pairRddFunctions = pool.get("org.apache.spark.rdd.PairRDDFunctions")

    // This block will finally replace all references to 'org.apache.spark.tez.TezContext' with 'org.apache.spark.SparkContext'
    // to finalize SparkContext instrumentation
    // java.lang.NoSuchMethodError: org.apache.spark.tez.TezContext$$anonfun$textFile$1.<init>(Lorg/apache/spark/SparkContext;)V  
    pairRddFunctionsAdapter.getNestedClasses().foreach {
      x =>
        x.replaceClassName("org.apache.spark.tez.adapter.PairRDDFunctionsAdapter", "org.apache.spark.rdd.PairRDDFunctions");
        x.toClass(systemClassLoader)
    }
    val pairRddTargetMethods = pairRddFunctions.getDeclaredMethods
    for (targetMethod <- pairRddTargetMethods) {
      if (targetMethod.getName() == "saveAsNewAPIHadoopDataset") {
        logDebug("Adapting PairRDDFunctions.saveAsNewAPIHadoopDataset for Tez")
        this.swapPairRddFunctionsMethodBody(targetMethod, pairRddFunctionsAdapter)
      } else if (targetMethod.getName() == "saveAsHadoopDataset") {
        logDebug("Adapting PairRDDFunctions.saveAsHadoopDataset for Tez")
        this.swapPairRddFunctionsMethodBody(targetMethod, pairRddFunctionsAdapter)
      } else if (targetMethod.getName() == "groupByKey") {
        logDebug("Adapting PairRDDFunctions.groupByKey for Tez")
        this.swapPairRddFunctionsMethodBody(targetMethod, pairRddFunctionsAdapter)
      }
    }

    this.klass = pool.toClass(pairRddFunctions, Thread.currentThread().getContextClassLoader())
  }

  /**
   *
   */
  private def swapPairRddFunctionsMethodBody(targetMethod: CtMethod, sourceClass: CtClass) {
    val desc = targetMethod.getMethodInfo().getDescriptor()
    try {
      val sourceMethod = sourceClass.getMethod(targetMethod.getName(), desc)
      targetMethod.setBody(sourceMethod, null)
      logDebug("Instrumented" + targetMethod.getMethodInfo.getDescriptor)
    } catch {
      case e: Throwable =>
        logDebug("skipping instrumentatoin of the " + targetMethod.getMethodInfo.getDescriptor)
      // ignore since methods that are not found based on CtMethod 
      // definitions are not going to be replaced
    }
  }
}
