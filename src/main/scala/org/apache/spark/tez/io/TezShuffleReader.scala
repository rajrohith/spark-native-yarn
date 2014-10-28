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
package org.apache.spark.tez.io

import org.apache.spark.shuffle.ShuffleReader
import org.apache.tez.runtime.api.LogicalInput
import org.apache.tez.runtime.api.Reader
import org.apache.tez.runtime.library.api.KeyValuesReader
import org.apache.tez.runtime.library.api.KeyValueReader
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.TaskContext
import org.apache.hadoop.io.Writable
import java.util.Map
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.conf.Configuration
import org.apache.tez.dag.api.TezConfiguration
import org.apache.spark.InterruptibleIterator
import org.apache.spark.util.NextIterator
import java.io.IOException
import scala.collection.JavaConverters._

/**
 * Implementation of Spark's ShuffleReader which delegates it's read functionality to Tez
 * This implementation is tailored for after-shuffle reads (e.g., ResultTask)
 */
 //TODO: combine common functionality in TezSourceReader into an abstract class
class TezShuffleReader[K, C](input: Map[Integer, LogicalInput], handle: BaseShuffleHandle[K, C, _])
  extends ShuffleReader[K, C] {
  private val inputIndex = input.keySet().iterator().next()
  private val reader = input.remove(this.inputIndex).getReader()
  
  val aggregatorFunction:Function2[Any, Any, Any] = {
    if (handle == null || handle.dependency == null && handle.dependency.aggregator == null){
      null
    }
    else {
      if (handle.dependency.aggregator.isDefined){
        handle.dependency.aggregator.get.mergeValue.asInstanceOf[Function2[Any, Any, Any]]
      }
      else {
        null
      }
    }
  }
  
  /**
   *
   */
  override def read(): Iterator[Product2[K, C]] = {
    new ShuffleIterator(this.reader, aggregatorFunction).asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
  }

  /**
   *
   */
  def stop = ()
}

/**
 *
 */
private class ShuffleIterator[K, C](reader: Reader, var aggregatorFunction:Function2[Any, Any, Any]) extends Iterator[Product2[Any, Any]] {
  var hasNextNeverCalled = true
  var containsNext = false;
  var shoudlCheckHasNext = false;
  
  private val kvReader =
    if (reader.isInstanceOf[KeyValuesReader]) {
      reader.asInstanceOf[KeyValuesReader]
    } else {
      reader.asInstanceOf[KeyValueReader]
    }
  
  private var currentValues:Iterator[Object] = _
  
  /**
   *
   */
  override def hasNext(): Boolean = {
    if (this.hasNextNeverCalled || shoudlCheckHasNext) {
      this.hasNextNeverCalled = false
      this.containsNext = this.doHasNext
    }
    this.containsNext
  }

  /**
   *
   */
  override def next(): Product2[Any, Any] = {
    if (hasNextNeverCalled) {
      this.hasNext
    }
    
    if (this.containsNext) {
      if (reader.isInstanceOf[KeyValuesReader]) {
        val reader = this.kvReader.asInstanceOf[KeyValuesReader]
        if (aggregatorFunction != null) {       
          this.currentValues = reader.getCurrentValues().iterator.asScala
          var mergedValue: Any = null
          for (value <- this.currentValues) {
            val vw = value.asInstanceOf[ValueWritable]
            val v = vw.getValue()
            if (mergedValue == null){
              mergedValue = v
            } else {
              mergedValue = aggregatorFunction(mergedValue, v)
            }
          }
          val key = reader.getCurrentKey.asInstanceOf[KeyWritable].getValue().asInstanceOf[Comparable[_]]
          val result = (key, mergedValue.asInstanceOf[Object])
          this.shoudlCheckHasNext = true
          result
        } else {
          if (this.currentValues == null){
             this.currentValues = reader.getCurrentValues().iterator.asScala
          }
          val key = reader.getCurrentKey.asInstanceOf[KeyWritable].getValue().asInstanceOf[Comparable[_]]
          val result = (key, this.currentValues.next.asInstanceOf[ValueWritable].getValue())
          if (!this.currentValues.hasNext){
             this.shoudlCheckHasNext = true
             this.currentValues = null
          }
          result
        }   
      } else {
        val reader = this.kvReader.asInstanceOf[KeyValueReader]
        val result = (reader.getCurrentKey, reader.getCurrentValue)
        this.shoudlCheckHasNext = true
        result
      }
      
    } else {
      throw new IllegalStateException("Reached the end of the iterator. " + 
          "Calling hasNext() prior to next() would avoid this exception")
    }
  }

  /**
   *
   */
  private def doHasNext(): Boolean = {
    this.shoudlCheckHasNext = false
    if (this.kvReader.isInstanceOf[KeyValuesReader]) {
      this.kvReader.asInstanceOf[KeyValuesReader].next
    } else {
      this.kvReader.asInstanceOf[KeyValueReader].next
    }
  }
}

/**
 * Wrapper over Tez's KeyValuesReader which uses semantics of java.util.Iterator
 * while giving you access to "current key" and "next value" contained in KeyValuesReader's ValuesIterator.
 */
private class KVSIterator(kvReader: KeyValuesReader) {
  var vIter:java.util.Iterator[_] = null

  /**
   * Checks if underlying reader contains more data to read.
   */
  def hasNext = {
    if (vIter != null && vIter.hasNext()) {
      true
    } else {
      if (kvReader.next()) {
        vIter = kvReader.getCurrentValues().iterator()
        true
      } else {
        false
      }
    }
  }

  /**
   * Returns the next value in the current ValuesIterator
   */
  def nextValue() = {
    vIter.next().asInstanceOf[TypeAwareWritable[_]].getValue()
  }

  /**
   * For this case 'ket' will always be represented by TypeAwareWritable (KeyWritable)
   * since its source is the result of YARN shuffle and the preceeding writer will
   * write all intermediate outputs as TypeAwareWritable for both keys and values.
   */
  def getCurrentKey = {
    kvReader.getCurrentKey().asInstanceOf[TypeAwareWritable[_]].getValue()
  }
}