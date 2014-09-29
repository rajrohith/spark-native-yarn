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
object SparkToTezAdapter extends Logging{

  private val unsafe = this.createUnsafe

  private val pool = ClassPool.getDefault();
  
  val systemClassLoader = ClassLoader.getSystemClassLoader()

  private val pairRddFunctionsAdapter = pool.get("org.apache.spark.tez.adapter.PairRDDFunctionsAdapter")
  private val pairRddFunctions = pool.get("org.apache.spark.rdd.PairRDDFunctions")
 
  // This block will finally replace all references to 'org.apache.spark.tez.TezContext' with 'org.apache.spark.SparkContext'
  // to finalize SparkContext instrumentation
  // java.lang.NoSuchMethodError: org.apache.spark.tez.TezContext$$anonfun$textFile$1.<init>(Lorg/apache/spark/SparkContext;)V  
  pairRddFunctionsAdapter.getNestedClasses().foreach{
    x => x.replaceClassName("org.apache.spark.tez.adapter.PairRDDFunctionsAdapter", "org.apache.spark.rdd.PairRDDFunctions"); 
    x.toClass(systemClassLoader)
  }
  
  /**
   * 
   */
  def adapt = {
    logInfo("Adapting PairRDDFunctions.saveAsNewAPIHadoopDataset for Tez")
    val pairRddTargetMethods = pairRddFunctions.getDeclaredMethods
    for (targetMethod <- pairRddTargetMethods){
      if (targetMethod.getName() == "saveAsNewAPIHadoopDataset"){
        this.swapPairRddFunctionsMethodBody(targetMethod)
      }
      else  if (targetMethod.getName() == "saveAsHadoopDataset"){
        this.swapPairRddFunctionsMethodBody(targetMethod)
      }
      else if (targetMethod.getName() == "groupByKey"){
        this.swapPairRddFunctionsMethodBody(targetMethod)
      }
    }
    val pairRddFunctionsBytes = this.pairRddFunctions.toBytecode()
    
    unsafe.defineClass(null, pairRddFunctionsBytes, 0, pairRddFunctionsBytes.length, systemClassLoader, systemClassLoader.getClass.getProtectionDomain())
  }

  /**
   *
   */
  private def swapPairRddFunctionsMethodBody(targetMethod: CtMethod) {
    val desc = targetMethod.getMethodInfo().getDescriptor()
    try {
      val sourceMethod = pairRddFunctionsAdapter.getMethod(targetMethod.getName(), desc)
      targetMethod.setBody(sourceMethod, null)
      logDebug("Instrumented" + targetMethod.getMethodInfo.getDescriptor)
    } catch {
      case e: Throwable => 
        logDebug("skipping instrumentatoin of the " + targetMethod.getMethodInfo.getDescriptor)
        				   // ignore since methods that are not found based on CtMethod 
        				   // definitions are not going to be replaced
    }
  }
  
  /**
   * 
   */
  private def createUnsafe = {
    val field = classOf[Unsafe].getDeclaredField("theUnsafe");
    field.setAccessible(true);
    field.get(null).asInstanceOf[Unsafe]
  }
}
