package org.apache.spark.tez.test.utils
import scala.reflect.runtime.universe._
import org.apache.spark.tez.TezJobExecutionContext
import scala.reflect.runtime.universe
import org.apache.spark.JobExecutionContext
import scala.reflect.ClassTag

/**
 * 
 */
object ReflectionUtils {

  def getFieldValue(instance:Any, fieldName:String):Any = {
    val fields = fieldName.split("\\.")
    var result:Any = instance
    for (field <- fields) {
      result = this.doGetFieldValue(result, field)
    }
    result
  }
  
  def setFieldValue(instance:Any, fieldName:String, newValue:Any) = {
    val fields = fieldName.split("\\.")
    var result:Any = instance
    for (i <- 0 until fields.length-1){
      result = this.doGetFieldValue(result, fields(i))
    }
    val field = result.getClass.getDeclaredField(fields.last)
    field.setAccessible(true)
    field.set(result, newValue)
  }
  
  private def doGetFieldValue(instance:Any, fieldName:String):Any = {
    val field = instance.getClass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(instance)
  }
}