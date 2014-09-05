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

class TezShuffleWriter[K, V](output:java.util.Map[Integer, LogicalOutput], handle: BaseShuffleHandle[K, V, _], context: TaskContext, combine:Boolean = true) extends ShuffleWriter[K, V] {
  private val kvOutput = output.values.iterator().next()
  private val kvWriter = kvOutput.getWriter().asInstanceOf[KeyValueWriter]
  private val serializer = SparkEnv.get.serializer.newInstance
  var set = false;

  /**
   * 
   */
  def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    /*
     * Combine and merge are different 
     * 	merge group values
     *  combine keys
     *  
     *  In the context of this 'write' they are mutually exclusive
     *  since 'merge' is specific to Tez's built in functionality to automatically group values by key so the only thing left is to combine grouped values
     *  	so it essentially delegates to the combiner
     *  combine is Spark's specific implementation which aggregates grouped values of a unique key
     *  so combine implies some grouped iterable which containes groyped values, so it dependes on merge or other func to group values
     */
    val iter = if (combine) this.buildCombinedIterator(records);
        	   else records
     
      

    var kw:DelegatingWritable = new DelegatingWritable
    var vw:BytesWritable = new BytesWritable
    var previousKey:Any = null
    var mergedValue: Any = null
    // if aggregator present and combiner absent
    val aggregate = handle != null && !combine
    val mergeFunction = if (aggregate) {
      handle.dependency.aggregator.get.mergeValue.asInstanceOf[Function2[Any, Any, Any]]
    }
    else {
      null
    }

    for (element <- iter) {
      if (aggregate) {
        if (previousKey == null) {
          previousKey = element._1
          mergedValue = element._2
        } else if (previousKey == element._1) {
          mergedValue = mergeFunction(mergedValue, element._2)
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
  private def buildCombinedIterator(records: Iterator[_ <: Product2[K, V]]) = {
    if (handle != null) {
        val dep = handle.dependency
        if (dep.aggregator.isDefined) {
          if (dep.mapSideCombine) {
            dep.aggregator.get.combineValuesByKey(records, context)
          } else {
            records
          }
        } else if (dep.aggregator.isEmpty && dep.mapSideCombine) {
          throw new IllegalStateException("Aggregator is empty for map-side combine")
        } else {
          records
        }
      } else {
        records
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