package org.apache.spark.tez.io

import org.apache.spark.shuffle.ShuffleWriter
import org.apache.hadoop.io.Writable
import org.apache.tez.runtime.api.LogicalOutput
import org.apache.tez.runtime.library.api.KeyValueWriter
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.TaskContext
import scala.reflect.runtime.universe._
import org.apache.spark.SparkEnv
import scala.collection.mutable.HashMap
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.spark.Logging
import org.apache.spark.tez.SparkUtils

/**
 * 
 */
class TezShuffleWriter[K, V, C](output:java.util.Map[Integer, LogicalOutput], 
    handle: BaseShuffleHandle[K, V, C], 
    context: TaskContext, 
    combine:Boolean = true) extends ShuffleWriter[K, V] with Logging {
  private val kvOutput = output.values.iterator().next()
  private val kvWriter = kvOutput.getWriter().asInstanceOf[KeyValueWriter]
  private var kw:Writable = null
  private val vw:ValueWritable = new ValueWritable
  /**
   * 
   */
  def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    val (keyValues, mergeFunction) = {
      val s = this.buildCombinedIterator(records, combine)
      (s._1, s._2)
    }

    this.sinkKeyValuesIterator(keyValues, mergeFunction)
  }

  /**
   *
   */
  private def sinkKeyValuesIterator(keyValues: Iterator[_ <: Product2[K, V]], mergeFunction: Function2[Any, Any, Any]) {
    var previousKey: Any = null
    var mergedValue: Any = null
    for (keyValue <- keyValues) {
      this.writeKeyValue(keyValue._1, keyValue._2)
    }
  }

  private def writeKeyValue(key: Any, value: Any) {
    this.toKeyWritable(key)
    this.toValueWritable(value)
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
  private def buildCombinedIterator(records: Iterator[_ <: Product2[K, V]], combine: Boolean): Tuple2[Iterator[_ <: Product2[K, V]], Function2[Any, Any, Any]] = {
    if (handle != null && handle.dependency.aggregator.isDefined) {
      val aggregator = handle.dependency.aggregator.get

      val mergeValueFunction = aggregator.mergeValue.asInstanceOf[Function2[Any, Any, Any]]
      if (combine) {
        val combiners = new HashMap[Any, Any]
        for (record <- records) {
          if (combiners.contains(record._1)) {
            val v1 = combiners.get(record._1).get
            val mergedValue = mergeValueFunction(v1, record._2)
            combiners.put(record._1, mergedValue)
          } else {
            combiners += record.asInstanceOf[Tuple2[K, V]]
          }
        }
        (combiners.iterator.asInstanceOf[Iterator[_ <: Product2[K, V]]], null)
      } else {
        (records, mergeValueFunction)
      }
    } else {
      (records, null)
    }
  }
  
  /**
   * 
   */
  private def toKeyWritable(value: Any) = {
    if (kw == null) {
      kw =
        if (value.isInstanceOf[Integer]) {
          new IntWritable(value.asInstanceOf[Integer])
        } else if (value.isInstanceOf[Long]) {
          new LongWritable(value.asInstanceOf[Long])
        } else if (value.isInstanceOf[String]) {
          new Text(value.toString)
        } else {
          throw new IllegalStateException("Unsupported type: " + value.getClass)
        }
    } 
    else {
      if (kw.isInstanceOf[Text]){
        kw.asInstanceOf[Text].set(value.toString)
      } 
      else if (kw.isInstanceOf[IntWritable]) {
        kw.asInstanceOf[IntWritable].set(value.asInstanceOf[Integer])
      }
      else if (kw.isInstanceOf[LongWritable]) {
        kw.asInstanceOf[LongWritable].set(value.asInstanceOf[Long])
      }
    }
  }
  
  /**
   * 
   */
  private def toValueWritable(value: Any) = {
    this.vw.setValue(value)
  }
}