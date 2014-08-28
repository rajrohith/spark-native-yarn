package org.apache.spark.tez
import scala.collection.Iterator
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.tez.runtime.api.Reader
import org.apache.tez.runtime.library.api.KeyValueWriter
import org.apache.tez.runtime.library.api.KeyValuesReader
import java.util.Map
import org.apache.tez.runtime.api.LogicalInput
import org.apache.tez.runtime.api.LogicalOutput
import org.apache.tez.runtime.library.api.KeyValueReader
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import java.lang.Iterable
import scala.collection.JavaConverters._
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.IntWritable
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.hadoop.io.NullWritable
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.Logging

/**
 * Implementation of Spark's ShuffleManager to support Spark's task connectivity 
 * to Tez's readers and writers
 * 
 */
class TezShuffleManager(val input:Map[Integer, LogicalInput], val output:Map[Integer, LogicalOutput]) extends ShuffleManager with Logging{
  logDebug("Creating Tez ShuffleManager")
  val key = new BytesWritable

  /**
   * 
   */
  def registerShuffle[K, V, C](
    shuffleId: Int,
    numMaps: Int,
    dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    null
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V] = {
    val serializer = SparkEnv.get.serializer.newInstance
    if (output.size() > 1){
      throw new UnsupportedOperationException("Multiple outputs are not supported yet.")
    }
    val kvWriter = output.values.iterator.next.getWriter.asInstanceOf[KeyValueWriter]
    val shuffleWriter = new ShuffleWriter[K, V] {
      /** Write a record to this task's output */
      
      def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
        for (record <- records) {
          if (record._1.isInstanceOf[Writable] && record._2.isInstanceOf[Writable]){ 
            kvWriter.write(record._1, record._2)
          }
          else {
            kvWriter.write(WritableDecoder.toWritable(record._1), WritableDecoder.toWritable(record._2))
          }
        }
      }

      /** Close this writer, passing along whether the map completed */
      def stop(success: Boolean): Option[MapStatus] = {
        Some(SparkUtils.createUnsafeInstance(classOf[MapStatus]))
      }
    }
    shuffleWriter
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  def getReader[K, C](
    handle: ShuffleHandle,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext): ShuffleReader[K, C] = {
    
    val reader = this.getReader

    val shuffleReader = new ShuffleReader[K, C] {
      /** Read the combined key-values for this reduce task */
      def read(): Iterator[Product2[K, C]] = {
        new Iterator[Product2[K, C]] {
          
          override def hasNext(): Boolean = {

            val hasNext =
              if (reader.isInstanceOf[KeyValueReader]) {
                reader.asInstanceOf[KeyValueReader].next()
              } else if (reader.isInstanceOf[KeyValuesReader]) {
                reader.asInstanceOf[KeyValuesReader].next()
              } else {
                throw new IllegalStateException("Unrecognized reader " + reader)
              }
            hasNext
          }

          override def next(): Product2[K, C] = {

            val (key, readerValue) =
              if (reader.isInstanceOf[KeyValueReader]) {
                val r = reader.asInstanceOf[KeyValueReader]
                val rKey = r.getCurrentKey()
                val rVal = r.getCurrentValue()
                (rKey, rVal)
              } else if (reader.isInstanceOf[KeyValuesReader]) {
                val r = reader.asInstanceOf[KeyValuesReader]
                val rKey = r.getCurrentKey()
                val rVal = r.getCurrentValues()
                (rKey, rVal)
              }

            var previousValue: Any = null
            if (readerValue.isInstanceOf[Iterable[_]]) {
              val iter = readerValue.asInstanceOf[Iterable[Writable]].asScala
              val mergeFunction = handle.asInstanceOf[BaseShuffleHandle[K, Any, C]].dependency.aggregator.get.mergeValue

              var acumulatedValue: Any = null

              for (value <- iter) {
                val decodedValue = WritableDecoder.fromWritable(value)
                acumulatedValue = mergeFunction(acumulatedValue.asInstanceOf[C], decodedValue.asInstanceOf[K])
              }
              previousValue = acumulatedValue
            } else {
              previousValue = readerValue
            }

            val product = (key.toString, previousValue)
            product.asInstanceOf[Product2[K, C]]
          }
        }
      }

      /** Close this reader */
      def stop(): Unit = ()
    }
    shuffleReader
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  def unregisterShuffle(shuffleId: Int) = ()

  /** Shut down this ShuffleManager. */
  def stop(): Unit = ()

  private def getReader(): Reader = {
    val inputIndex = input.keySet().iterator().next()
    val reader = input.remove(inputIndex).getReader()
    reader
  }
}

/**
 *
 */
private object WritableDecoder {
  /**
   *
   */
  def fromWritable(writable: Writable) = {
    if (writable.isInstanceOf[LongWritable]) {
      writable.asInstanceOf[LongWritable].get
    } else if (writable.isInstanceOf[IntWritable]) {
      writable.asInstanceOf[IntWritable].get
    } else if (writable.isInstanceOf[Text]) {
      writable.toString
    } else {
      throw new IllegalStateException("Unsupported writable " + writable)
    }
  }
  
  /**
   *
   */
  def toWritable(value: Any) = {
    if (value.isInstanceOf[Integer]) {
      new IntWritable(value.asInstanceOf[Integer])
    } else if (value.isInstanceOf[Long]) {
      new LongWritable(value.asInstanceOf[Long])
    } else if (value.isInstanceOf[String]) {
      new Text(value.asInstanceOf[String])
    } else {
      throw new IllegalStateException("Unsupported type to convert to Writable " + value.getClass.getName())
    }
  }
}