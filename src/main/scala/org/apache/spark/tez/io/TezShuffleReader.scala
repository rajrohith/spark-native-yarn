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

/**
 * Implementation of Spark's ShuffleReader tailored for Tez which means its is aware of
 * two different readers provided by Tez; KeyValueReader and KeyValuesReader
 */
class TezShuffleReader[K, C](input: Map[Integer, LogicalInput], handle: BaseShuffleHandle[K, _, C], context: TaskContext, combine: Boolean = true)
  extends ShuffleReader[K, C] {
  private val inputIndex = input.keySet().iterator().next()
  private val reader = input.remove(inputIndex).getReader()

  /**
   *
   */
  override def read(): Iterator[Product2[K, C]] = {
    val iterator = new TezIterator(reader).asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
    iterator
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

  var kvReader: KeyValueReader = null
  var kvsReader: KVSIterator = null
  if (reader.isInstanceOf[KeyValueReader]) {
    kvReader = reader.asInstanceOf[KeyValueReader]
  } else {
    kvsReader = new KVSIterator(reader.asInstanceOf[KeyValuesReader])
  }

  /**
   *
   */
  override def hasNext(): Boolean = {
    if (kvReader != null) {
      kvReader.next
    } else {
      kvsReader.hasNext
    }
  }

  /**
   *
   */
  override def next(): Product2[Any, Any] = {
    if (kvReader != null) {
      (kvReader.getCurrentKey(), kvReader.getCurrentValue())
    } else {
      (kvsReader.getCurrentKey, kvsReader.nextValue)
    }
  }
}

/**
 * Wrapper over Tez's KeyValuesReader which uses semantics of java.util.Iterator
 * while giving you access to "current key" and "next value" contained in KeyValuesReader's ValuesIterator.
 */
private class KVSIterator(kvReader: KeyValuesReader) {
  var vIter = kvReader.getCurrentValues().iterator()

  /**
   * Checks if underlying reader contains more data to read.
   */
  def hasNext = {
    var next =
      if (vIter.hasNext()) {
        true
      } else {
        if (kvReader.next()) {
          vIter = kvReader.getCurrentValues().iterator()
          true
        } else {
          false
        }
      }
    next
  }

  /**
   * Returns the next value in the current ValuesIterator
   */
  def nextValue() = {
    this.getSimplType(vIter.next())
  }

  /**
   * 
   */
  def getCurrentKey = {
    this.getSimplType(kvReader.getCurrentKey())
  }

  /**
   * 
   */
  private def getSimplType(s: Any): Any = {
    if (s.isInstanceOf[Writable]) {
      if (s.isInstanceOf[Text]) {
        s.toString
      } else if (s.isInstanceOf[IntWritable]) {
        s.asInstanceOf[IntWritable].get
      } else if (s.isInstanceOf[LongWritable]) {
        s.asInstanceOf[LongWritable].get
      } else if (s.isInstanceOf[ValueWritable]) {
        s.asInstanceOf[ValueWritable].getValue
      } else {
        throw new IllegalArgumentException("Unrecognized writable: " + s)
      }
    } else {
      s
    }
  }
}