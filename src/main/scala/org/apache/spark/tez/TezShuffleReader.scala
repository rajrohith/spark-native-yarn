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

/**
 * 
 */
class TezShuffleReader[K, C](input: java.util.Map[Integer, LogicalInput], var handle: BaseShuffleHandle[K, _, C], context: TaskContext, combine:Boolean = true) extends ShuffleReader[K, C] {
  val serializer = SparkEnv.get.serializer.newInstance
  val inputIndex = input.keySet().iterator().next()
  val reader = input.remove(inputIndex).getReader()

  /**
   *
   */
  def read(): Iterator[Product2[K, C]] = {
    val iterator =
      if (combine) {
        this.combinedIterator(new TezIterator(reader))
      } else {
        new TezIterator(reader).asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
      }

    iterator
  }
  
  /**
   * 
   */
  def combinedIterator(iter:Iterator[Product2[K, C]]):Iterator[Product2[K, C]] = {
    if (handle != null) {
      val dep = handle.dependency
      if (dep.aggregator.isDefined) {
        val mergeCombiners = dep.aggregator.get.mergeCombiners
        val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
        combiners.insertAll(iter)
        combiners.iterator
      } else {
        // Convert the Product2s to pairs since this is what downstream RDDs currently expect
        iter.asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
      }
    }
    else {
      iter.asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
    }
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
  val serializer = SparkEnv.get.serializer.newInstance
  
  val wrappedReader = 
    if (reader.isInstanceOf[KeyValueReader]){
    	reader
    }
    else {
      new KVSIterator(reader.asInstanceOf[KeyValuesReader])
//      reader
    }
  
  /**
   * 
   */
  override def hasNext(): Boolean = {
    var valuesCount = 0
    val hasNext =
      if (wrappedReader.isInstanceOf[KeyValueReader]) {
        reader.asInstanceOf[KeyValueReader].next()
      }
      else {
        val keyType = ExecutionContext.getObjectRegistry().get("KEY_TYPE").asInstanceOf[Class[_]]
        if (!DelegatingWritable.initialized()) {
          DelegatingWritable.setType(keyType);
        }
        wrappedReader.asInstanceOf[KVSIterator].hasNext
      }
    hasNext
  }

  /**
   * 
   */
  override def next(): Product2[K, C] = {
    val (key, readerValue) =
      if (wrappedReader.isInstanceOf[KeyValueReader]) {
        val r = wrappedReader.asInstanceOf[KeyValueReader]
        val rKey = r.getCurrentKey()
        val rVal = r.getCurrentValue()
        (rKey, rVal)
      } 

      else {
        val r = wrappedReader.asInstanceOf[KVSIterator]
        val rKey = r.currentKey
        val rVal = r.nextValue
        println("Deserialized: " + rKey + "-" + rVal)
        (rKey, rVal)
      }

    println(key + "-" + readerValue)
    
    (key, readerValue).asInstanceOf[Product2[K, C]]
  }
}

private class KVSIterator(kvReader:KeyValuesReader) {
  val serializer = SparkEnv.get.serializer.newInstance
  var vIter:java.util.Iterator[_] = null
  
  def hasNext = {
    var next = false;
    if (vIter == null){
      if (kvReader.next()){
        vIter = kvReader.getCurrentValues().iterator()
        next = true
      }
      else {
        next = false
      }
    }
    else {
      if (vIter.hasNext()){
        next = true
      }
      else {
        if (kvReader.next()){
          vIter = kvReader.getCurrentValues().iterator()
          next = true
        }
        else {
          next = false
        }
      }
    }
    next
  }
  
  def nextValue() = {
    val valueBuffer = ByteBuffer.wrap(vIter.next().asInstanceOf[BytesWritable].getBytes())
    val value = serializer.deserialize[Any](valueBuffer)
    value
  }
  
  def currentKey = {
    kvReader.getCurrentKey().asInstanceOf[DelegatingWritable].getValue()
  }
}