package org.apache.spark

import scala.collection.JavaConversions
import java.io.ObjectInputStream
import java.io.FileInputStream
import scala.collection.mutable.ArrayOps
import org.apache.tez.runtime.library.api.KeyValueWriter
import org.springframework.core.io.ClassPathResource
import java.net.URLClassLoader
import java.io.File
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

class FunctionHelper(val vertexId:Int) {
  println("CREATING FunctionHelper for " + vertexId)
  var deserializedFunctioin:Function1[Any, ArrayOps[_]] = null;
  
  val classpath = Thread.currentThread().getContextClassLoader().asInstanceOf[URLClassLoader]getURLs();
  var functionFile:File = null;
  for (resource <- classpath){
    if (resource.getPath().endsWith("_" + vertexId + ".ser")){
      functionFile = new File(resource.toURI())
    }
//    println("resource: " + resource)
  }
  try {
    val is = new ObjectInputStream(new FileInputStream(functionFile))
    deserializedFunctioin = is.readObject().asInstanceOf[Function[Any, ArrayOps[_]]]
  } catch {
    case e: Exception => e.printStackTrace()
  }

  def applyFunction1(value:Any, keyValueWriter:KeyValueWriter):Unit = {
    val results = deserializedFunctioin(value)
    for (result <- results){
      if (result.isInstanceOf[Tuple2[_,_]]){
        val tupleResult = result.asInstanceOf[Tuple2[_,_]]
        keyValueWriter.write(new Text(tupleResult._1.asInstanceOf[String]), new IntWritable(tupleResult._2.asInstanceOf[Integer]))
      }
    }
  }
}