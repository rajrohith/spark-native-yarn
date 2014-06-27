package com.hortonworks.spark.tez

import java.io.File
import java.io.FileInputStream
import java.io.ObjectInputStream
import java.net.URLClassLoader
import scala.collection.JavaConversions
import scala.collection.mutable.ArrayOps
import org.apache.tez.runtime.library.api.KeyValueWriter
import org.springframework.core.io.ClassPathResource

/**
 * 
 */
class FunctionHelper(val vertexId:Int) {
  println("CREATING FunctionHelper for " + vertexId)
  var deserializedFunctioin:Function1[Any, ArrayOps[_]] = null;
  
  val classpath = Thread.currentThread().getContextClassLoader().asInstanceOf[URLClassLoader]getURLs();
  val serializedFunction = new ClassPathResource("UserFunction_" + vertexId + ".ser")

  try {
    val is = new ObjectInputStream(serializedFunction.getInputStream())
    deserializedFunctioin = is.readObject().asInstanceOf[Function[Any, ArrayOps[_]]]
  } catch {
    case e: Exception => e.printStackTrace()
  }

  /**
   * 
   */
  def applyFunction1(value:Any, keyValueWriter:KeyValueWriter):java.lang.Iterable[_] = {
    JavaConversions.asJavaIterable(deserializedFunctioin(value).toIterable)
  }
}