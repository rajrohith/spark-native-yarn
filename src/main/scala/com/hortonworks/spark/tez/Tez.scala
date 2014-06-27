package com.hortonworks.spark.tez

import java.io.File
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import scala.reflect.ClassTag
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.springframework.util.ReflectionUtils
import com.hortonworks.tez.spark.TezContext
import com.hortonworks.tez.spark.TezDagBuilder;
import org.apache.spark.util.ClosureCleaner


/**
 * 
 */
trait Tez extends SparkContext {
  
  var inputPath = ""
  
  override def textFile(path: String, minPartitions: Int = defaultMinPartitions): RDD[String] = {
    inputPath = path
    val dummyFile = File.createTempFile("dummy", ".tmp"); 
    super.textFile("file:///" + dummyFile.getAbsolutePath(), minPartitions)
  }
  
  /**
   * 
   */
  override def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    println("Intercepted Spark execution in favor of Tez")
    
    val sparkContext = this.extractSparkContext(rdd);
    val applicationName = sparkContext.appName;
    
    println("Building Tez DAG for " + applicationName)
    val dependencies = flatten(rdd)(tezPortable)
    val configuration = new YarnConfiguration();
    println("Resource manager is at: " + configuration.get("yarn.resourcemanager.address"))
    val tezContext = new TezContext(configuration, applicationName)
    val tezDagBuilder = new TezDagBuilder(tezContext, inputPath, null)
    
    val function = this.extractUserFunction(dependencies(1))
    
    this.cleanFunction(function);

    val operationByteArrayStream = new FileOutputStream("bin/UserFunction_0.ser", true)
	val oos = new ObjectOutputStream(operationByteArrayStream);
	oos.writeObject(function);
	oos.close()
	
	println("Building DAG for Tez");
	tezDagBuilder.build()
	
	println("Submitting DAG to Tez")
	tezDagBuilder.run()
	
    Array[U]()
  }
  
  def tezPortable(dependency:RDD[_]): Boolean = {
    dependency.getClass().getSimpleName().endsWith("edRDD")
  }
  
  /**
   * 
   */
  private def cleanFunction(function:AnyRef){
    val closureCleaner = Class.forName("org.apache.spark.util.ClosureCleaner")
    val cleanMethod =  closureCleaner.getDeclaredMethod("clean", classOf[AnyRef])
    cleanMethod.invoke(null, function)
  }

  /**
   * Will flatten Spark RDD dependencies.
   */
  // TODO Figure out the scenarios where there is more 
  // then one dependency in rdd.dependencies
  def flatten(rdd: RDD[_], l: List[RDD[_]] = Nil)(f: RDD[_] => Boolean): List[RDD[_]] = {
    if (rdd.dependencies.size > 0) {
      flatten(rdd.dependencies(0).rdd, if (f(rdd)) (rdd :: l) else l)(f)
    } 
    else {
      if (f(rdd)) (rdd :: l) else l
    }
  }
  
  /**
   * 
   */
  def extractSparkContext(rdd:RDD[_]):SparkContext = {
    val scField = ReflectionUtils.findField(rdd.getClass(), "sc")
    scField.setAccessible(true)
    scField.get(rdd).asInstanceOf[SparkContext]
  }
  
  /**
   * 
   */
  def extractUserFunction(rdd:RDD[_]):AnyRef = {
    val scField = ReflectionUtils.findField(rdd.getClass(), "f")
    if (scField != null) {
      scField.setAccessible(true)
      scField.get(rdd)
    }
    else {
      None
    }
  }
  
}