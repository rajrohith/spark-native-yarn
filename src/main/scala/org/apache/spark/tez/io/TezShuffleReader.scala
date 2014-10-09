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

/**
 * Implementation of Spark's ShuffleReader tailored for Tez which means its is aware of
 * two different readers provided by Tez; KeyValueReader and KeyValuesReader
 */
class TezShuffleReader[K, C](input: Map[Integer, LogicalInput])
  extends ShuffleReader[K, C] {
  private val inputIndex = input.keySet().iterator().next()
  private val reader = input.remove(this.inputIndex).getReader()

  /**
   *
   */
  override def read(): Iterator[Product2[K, C]] = {
    new TezIterator(this.reader).asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
  }

  /**
   *
   */
  def stop = ()
}
/**
 *
 */
private class TezIterator[K, C](reader: Reader) extends Iterator[Product2[Any, Any]] {

  val (kvReader: Option[KeyValueReader], kvsReader: Option[KVSIterator]) =
    if (reader.isInstanceOf[KeyValueReader]) {
      (Some(reader.asInstanceOf[KeyValueReader]), None)
    } else {
      (None, new Some(new KVSIterator(reader.asInstanceOf[KeyValuesReader])))
    }

  /**
   *
   */
  override def hasNext(): Boolean = {
    if (this.kvReader.isDefined) {
      this.kvReader.get.next
    } else {
      this.kvsReader.get.hasNext
    }
  }

  /**
   *
   */
  override def next(): Product2[Any, Any] = {
    if (this.kvReader.isDefined) {
      (this.kvReader.get.getCurrentKey(), this.kvReader.get.getCurrentValue())
    } else {
      (this.kvsReader.get.getCurrentKey, this.kvsReader.get.nextValue)
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