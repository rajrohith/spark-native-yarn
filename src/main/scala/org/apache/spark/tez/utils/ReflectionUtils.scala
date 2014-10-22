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
import java.lang.reflect.Field
import org.apache.spark.rdd.RDD

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

  /**
   * 
   */
  def findField(clazz: Class[_], name: String, _type: Class[_]): Field = {
    var searchType = clazz
    while (!classOf[Object].equals(searchType) && searchType != null) {
      val fields = searchType.getDeclaredFields()
      for (field <- fields) {
        if ((name == null || name.equals(field.getName())) && (_type == null || _type.equals(field.getType()))) {
          return field;
        }
      }
      searchType = searchType.getSuperclass();
    }
    null
  }
  
  /**
   * 
   */
  def rddClassTag(rdd:RDD[_]):ClassTag[_] = {
    val classTags = classOf[RDD[_]].getDeclaredFields().filter(_.getType().isAssignableFrom(classOf[ClassTag[_]]))
    if (classTags.length == 1){
      val f = classTags(0)
      f.setAccessible(true)
      f.get(rdd).asInstanceOf[ClassTag[_]]
    } else {
      throw new IllegalStateException("Failed to determine ClassTag for " + rdd)
    }
  }
  
  /**
   * 
   */
  private def doGetFieldValue(instance: Any, fieldName: String): Any = {
    val field = this.findField(instance.getClass, fieldName, null)
    if (field != null) {
      field.setAccessible(true)
      field.get(instance)
    }
  }
}