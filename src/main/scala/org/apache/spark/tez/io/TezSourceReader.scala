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
 * This implementation is tailored for before-shuffle reads (e.g., Reading initial source)
 */
//TODO: combine common functionality in TezShuffleReader into an abstract class
class TezSourceReader[K, C](input: Map[Integer, LogicalInput])
  extends ShuffleReader[K, C] {

  private val inputIndex = input.keySet().iterator().next()
  private val reader = input.remove(this.inputIndex).getReader().asInstanceOf[KeyValueReader]

  /**
   *
   */
  override def read(): Iterator[Product2[K, C]] = {
    new SourceIterator(this.reader).asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
  }

  /**
   *
   */
  def stop = ()
}
/**
 *
 */
private class SourceIterator[K, C](reader: KeyValueReader) extends Iterator[Product2[Any, Any]] {
  var hasNextNeverCalled = true
  var containsNext = false;
  var shoudlCheckHasNext = false;
  private var currentValues: Iterator[Object] = _

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
      val result = (reader.getCurrentKey, reader.getCurrentValue)
      this.shoudlCheckHasNext = true
      result
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
      this.reader.next
  }
}