package org.apache.spark.tez.instrument

import org.apache.spark.rdd.RDD
import javassist.ClassPool
import javassist.CtClass
import javassist.CtMethod
import sun.misc.Unsafe
import java.util.HashMap
import javassist.ClassMap
import javassist.Modifier
import javassist.CtField
import org.apache.tez.dag.api.TezConfiguration

/**
 * 
 */
object TezInstrumentationAgent {

  val unsafe = {
    val field = classOf[Unsafe].getDeclaredField("theUnsafe");
    field.setAccessible(true);
    field.get(null).asInstanceOf[Unsafe]
  }
  
  private val pool = ClassPool.getDefault();
  
  private val tezContextClass = pool.get("org.apache.spark.tez.TezContext")
  private val sparkContextClass = pool.get("org.apache.spark.SparkContext")
  
 
  // need to do more cleanup on nested classes to avoid
  // java.lang.NoSuchMethodError: org.apache.spark.tez.TezContext$$anonfun$textFile$1.<init>(Lorg/apache/spark/SparkContext;)V
  tezContextClass.getNestedClasses().foreach{
    x => x.replaceClassName("org.apache.spark.tez.TezContext", "org.apache.spark.SparkContext"); 
    x.toClass()
  }

  println("replaced")
  /**
   * 
   */
  def instrument = {
//    val baseConstructor = this.sparkContextClass.getConstructor("(Lorg/apache/spark/SparkConf;)V")
//    val instrConstructor = this.tezContextClass.getConstructor("(Lorg/apache/spark/SparkConf;)V")
//    baseConstructor.setBody(instrConstructor, null)
    
    
    val targetMethods = sparkContextClass.getDeclaredMethods
    for (targetMethod <- targetMethods){
      if (targetMethod.getName() == "parallelize"){
        this.swapMethodBody(targetMethod)
      }
      else if (targetMethod.getName() == "newAPIHadoopFile"){
        this.swapMethodBody(targetMethod)
      }
      else if (targetMethod.getName() == "textFile"){
        this.swapMethodBody(targetMethod)
      }
      else if (targetMethod.getName() == "runJob"){
        this.swapMethodBody(targetMethod)
      }
//      else if (targetMethod.getName() == "defaultParallelism"){
//        this.swapMethodBody(targetMethod)
//      }
    }
    
    println("getting byte code");
    val scBytes = this.sparkContextClass.toBytecode()
    unsafe.defineClass(null, scBytes, 0, scBytes.length, this.getClass.getClassLoader(), this.getClass.getProtectionDomain())
  }

  /**
   *
   */
  private def swapMethodBody(targetMethod: CtMethod) {
    val desc = targetMethod.getMethodInfo().getDescriptor()
    try {
      val sourceMethod = tezContextClass.getMethod(targetMethod.getName(), desc)
      targetMethod.setBody(sourceMethod, null)
    } catch {
      case _: Throwable =>
    }

  }
}
