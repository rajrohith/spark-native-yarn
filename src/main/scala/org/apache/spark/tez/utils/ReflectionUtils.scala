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
package org.apache.spark.tez.utils

import scala.reflect.runtime.universe._
import org.apache.spark.tez.TezJobExecutionContext
import scala.reflect.runtime.universe
import org.apache.spark.JobExecutionContext
import scala.reflect.ClassTag

/**
 * 
 */
object ReflectionUtils {

  /**
   * 
   */
  def getFieldValue(instance:Any, fieldName:String):Any = {
    val fields = fieldName.split("\\.")
    var result:Any = instance
    for (field <- fields) {
      result = this.doGetFieldValue(result, field)
    }
    result
  }
  
  /**
   * 
   */
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
    try {
      val field = instance.getClass.getDeclaredField(fieldName)
      field.setAccessible(true)
      field.get(instance)
    } catch {
      case nsfe:NoSuchFieldException => null
      case e:Exception => throw new IllegalStateException(e)
    }
  }
}