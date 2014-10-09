package org.apache.spark.tez.test.utils
import scala.reflect.runtime.universe._
import org.apache.spark.tez.TezJobExecutionContext
import scala.reflect.runtime.universe
import org.apache.spark.JobExecutionContext
import scala.reflect.ClassTag

object ReflectionUtils {
  
  val typeMirror = runtimeMirror(this.getClass.getClassLoader)

  def getFieldValue[T:TypeTag:ClassTag](instance:T, fieldName:String):Any = {
    val instanceMirror = typeMirror.reflect(instance)
    val fieldX = typeOf[T].declaration(newTermName(fieldName)).asTerm.accessed.asTerm
    val fmX = instanceMirror.reflectField(fieldX)
    fmX.get
  }
}