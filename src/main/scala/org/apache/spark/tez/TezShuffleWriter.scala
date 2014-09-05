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
  var set = false;

  /**
   * 
   */
  def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    
    val (iterator, mergeFunction) =
      if (combine){
        this.buildCombinedIterator(records)
      }
      else {
        (records, null.asInstanceOf[Function2[C,V,C]])
      }


    var kw:DelegatingWritable = new DelegatingWritable
    var vw:BytesWritable = new BytesWritable
    var previousKey:Any = null
    var mergedValue: Any = null
    
    val aggregate = mergeFunction != null
    
    for (element <- iterator) {
      if (aggregate) {
        if (previousKey == null) {
          previousKey = element._1
          mergedValue = element._2
        } else if (previousKey == element._1) {
          println("REDUCING IN WRITER")
          mergedValue = mergeFunction(mergedValue.asInstanceOf[C], element._2)
        } else {
          println("Writing out to FS: " + previousKey + "-" + mergedValue)
          this.prepareTypeIfNecessary(element)
          kw.setValue(previousKey)
          val bytes = serializer.serialize[Any](mergedValue).array
          vw.set(bytes, 0, bytes.length)
          kvWriter.write(kw, vw)

          previousKey = element._1
          mergedValue = element._2
        }
      } else {
        println("Writing out to FS: " + element)
        this.prepareTypeIfNecessary(element)
        kw.setValue(element._1)
        val bytes = serializer.serialize[Any](element._2).array
        vw.set(bytes, 0, bytes.length)
        kvWriter.write(kw, vw)
      }
    }
    // last element need to be flushed
    if (previousKey != null) {
      kw.setValue(previousKey)
      val bytes = serializer.serialize[Any](mergedValue).array
      vw.set(bytes, 0, bytes.length)
      kvWriter.write(kw, vw)
    }
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
    if (!set) {
      DelegatingWritable.setType(element._1.getClass);
      ExecutionContext.getObjectRegistry().cacheForSession("KEY_TYPE", element._1.getClass)
      set = true
    }
  }
}