package org.apache.spark.tez.test.utils
import scala.reflect.runtime.universe._
import org.apache.spark.tez.TezJobExecutionContext
import scala.reflect.runtime.universe

object ReflectionUtils {
  
  val typeMirror = runtimeMirror(this.getClass.getClassLoader)

  def getFieldValue(instance:Any, fieldName:String):Any = {
    val instanceMirror = typeMirror.reflect(instance)
    val fieldX = typeOf[TezJobExecutionContext].declaration(newTermName(fieldName)).asTerm.accessed.asTerm
    val fmX = instanceMirror.reflectField(fieldX)
    fmX.get
  }
}