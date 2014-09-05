package org.apache.spark.tez

import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.tez.io.DelegatingWritable
import org.apache.hadoop.io.Writable
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.tez.runtime.api.LogicalOutput
import org.apache.tez.runtime.library.api.KeyValueWriter
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.BaseShuffleHandle
import breeze.linalg.shuffle
import org.apache.spark.TaskContext
import scala.reflect.runtime.universe._
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.SparkEnv
import java.nio.ByteBuffer
import org.apache.spark.util.collection.ExternalAppendOnlyMap

/**
 * 
 */
class TezShuffleWriter[K, V, C](output:java.util.Map[Integer, LogicalOutput], handle: BaseShuffleHandle[K, V, C], context: TaskContext, combine:Boolean = true) extends ShuffleWriter[K, V] {
  private val kvOutput = output.values.iterator().next()
  private val kvWriter = kvOutput.getWriter().asInstanceOf[KeyValueWriter]
  private val serializer = SparkEnv.get.serializer.newInstance
  private val kw:DelegatingWritable = new DelegatingWritable
  private val vw:BytesWritable = new BytesWritable
  var set = false;

  /**
   * 
   */
  def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    val (keyValues, mergeFunction) =
      if (combine){
        this.buildCombinedIterator(records)
      }
      else {
        (records, null.asInstanceOf[Function2[C,V,C]])
      }
    
    this.sinkKeyValuesIterator(keyValues, mergeFunction)
  }
  
  /**
   * 
   */
  private def sinkKeyValuesIterator(keyValues: Iterator[_ <: Product2[K, V]], mergeFunction:Function2[C,V,C]) {
    var previousKey:Any = null
    var mergedValue: Any = null
    for (keyValue <- keyValues) {
      this.prepareTypeIfNecessary(keyValue)
      if (mergeFunction != null) {
        if (previousKey == null) {
          previousKey = keyValue._1
          mergedValue = keyValue._2
        } else if (previousKey == keyValue._1) {
          println("REDUCING IN WRITER")
          mergedValue = mergeFunction(mergedValue.asInstanceOf[C], keyValue._2)
        } else {
          this.writeKeyValue(previousKey, mergedValue)

          previousKey = keyValue._1
          mergedValue = keyValue._2
        }
      } else {
        this.writeKeyValue(keyValue._1, keyValue._2)
      }
    }
    // last element need to be flushed
    if (previousKey != null) {
      this.writeKeyValue(previousKey, mergedValue)
    }
  }

  private def writeKeyValue(key: Any, value: Any) {
    kw.setValue(key)
    val bytes = serializer.serialize[Any](value).array
    vw.set(bytes, 0, bytes.length)
    kvWriter.write(kw, vw)
  }

  /**
   * 
   */
  def stop(success: Boolean): Option[MapStatus] = {
    Some(SparkUtils.createUnsafeInstance(classOf[MapStatus]))
  }

  /**
   *
   */
  private def buildCombinedIterator(records: Iterator[_ <: Product2[K, V]]):Tuple2[Iterator[_ <: Product2[K, V]], Function2[C,V,C]] = {
    if (handle != null && handle.dependency.aggregator.isDefined) {
      val aggregator = handle.dependency.aggregator.get

      val createCombinerFunction = aggregator.createCombiner
      val mergeValueFunction = aggregator.mergeValue
      val merCombinersFunction = aggregator.mergeCombiners
      val combiners = new ExternalAppendOnlyMap[K, V, C](createCombinerFunction, mergeValueFunction, merCombinersFunction)
      combiners.insertAll(records)
      (combiners.iterator.asInstanceOf[Iterator[_ <: Product2[K, V]]], mergeValueFunction)
    } else {
      (records, null.asInstanceOf[Function2[C,V,C]])
    }
  }

  /**
   * 
   */
  private def prepareTypeIfNecessary(element: Product2[_, _]) {
    if (!this.set) {
      DelegatingWritable.setType(element._1.getClass);
      ExecutionContext.getObjectRegistry().cacheForSession("KEY_TYPE", element._1.getClass)
      this.set = true
    }
  }
}