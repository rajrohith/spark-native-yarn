package org.apache.spark.tez

import org.apache.spark.shuffle.ShuffleReader
import org.apache.tez.runtime.api.LogicalInput
import org.apache.spark.SparkEnv
import org.apache.tez.runtime.api.Reader
import org.apache.tez.runtime.library.api.KeyValuesReader
import org.apache.spark.tez.io.DelegatingWritable
import org.apache.tez.runtime.library.api.KeyValueReader
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.InterruptibleIterator
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.TaskContext
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.BytesWritable
import java.nio.ByteBuffer
import org.apache.hadoop.io.ByteWritable
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import java.util.Map

/**
 * Implementation of Spark's ShuffleReader tailored for Tez which means its is aware of
 * two different readers provided by Tez; KeyValueReader and KeyValuesReader
 */
class TezShuffleReader[K, C](input: Map[Integer, LogicalInput], handle: BaseShuffleHandle[K, _, C], context: TaskContext, combine:Boolean = true) 
								extends ShuffleReader[K, C] {
  private val serializer = SparkEnv.get.serializer.newInstance
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
private class TezIterator[K, C](reader: Reader) extends Iterator[Product2[K, C]] {
  private val serializer = SparkEnv.get.serializer.newInstance
  
  private val wrappedReader = 
    if (reader.isInstanceOf[KeyValueReader]){
    	reader
    }
    else {
      new KVSIterator(reader.asInstanceOf[KeyValuesReader])
    }
  
  /**
   * 
   */
  override def hasNext(): Boolean = {
    val hasNext =
      if (this.wrappedReader.isInstanceOf[KeyValueReader]) {
        reader.asInstanceOf[KeyValueReader].next()
      }
      else {
        val keyType = ExecutionContext.getObjectRegistry().get("KEY_TYPE").asInstanceOf[Class[_]]
        if (!DelegatingWritable.initialized()) {
          DelegatingWritable.setType(keyType);
        }
        this.wrappedReader.asInstanceOf[KVSIterator].hasNext
      }
    hasNext
  }

  /**
   * 
   */
  override def next(): Product2[K, C] = {
    val (key, value) =
      if (wrappedReader.isInstanceOf[KeyValueReader]) {
        val kvr = wrappedReader.asInstanceOf[KeyValueReader]
        (kvr.getCurrentKey(), kvr.getCurrentValue())
      } else {
        val kvi = wrappedReader.asInstanceOf[KVSIterator]
        (kvi.currentKey, kvi.nextValue)
      }
    
    (key, value).asInstanceOf[Product2[K, C]]
  }
}

/**
 * Wrapper over Tez's KeyValuesReader which uses semantics of java.util.Iterator
 * while giving you access to "current key" and "next value" contained in KeyValuesReader's ValuesIterator. 
 */
private class KVSIterator(kvReader: KeyValuesReader) {
  val serializer = SparkEnv.get.serializer.newInstance
  var vIter: java.util.Iterator[_] = kvReader.getCurrentValues().iterator()

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
    val valueBuffer = ByteBuffer.wrap(vIter.next().asInstanceOf[BytesWritable].getBytes())
    val value = serializer.deserialize[Any](valueBuffer)
    value
  }

  /**
   * Returns current key associated with the current ValuesIterator
   */
  def currentKey = {
    kvReader.getCurrentKey().asInstanceOf[DelegatingWritable].getValue()
  }
}