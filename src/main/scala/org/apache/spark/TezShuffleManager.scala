package org.apache.spark

import java.nio.ByteBuffer
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
import com.hortonworks.spark.tez.TezThreadLocalContext
import com.hortonworks.spark.tez.KeyValueReaderWrapper

class TezShuffleManager extends ShuffleManager {
  val key = new BytesWritable

  def registerShuffle[K, V, C](
    shuffleId: Int,
    numMaps: Int,
    dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    println("registerShuffle")
    null
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V] = {
    println("getWriter")
    val serializer = SparkEnv.get.serializer.newInstance
    val kvWriter = TezThreadLocalContext.getWriter.asInstanceOf[KeyValueWriter]
    val shuffleWriter = new ShuffleWriter[K, V] {
      /** Write a record to this task's output */
      def write(record: Product2[K, V]): Unit = {
        val bwValue = new BytesWritable(serializer.serialize(record).array())
        kvWriter.write(key, bwValue)
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
    println("getReader")
    val reader = TezThreadLocalContext.getReader.asInstanceOf[Reader]
    val serializer = SparkEnv.get.serializer.newInstance

    val shuffleReader = new ShuffleReader[K, C] {
      /** Read the combined key-values for this reduce task */
      def read(): Iterator[Product2[K, C]] = {
        val kvsReader = new KeyValueReaderWrapper(reader.asInstanceOf[KeyValuesReader])
        new Iterator[Product2[K, C]] {

          override def hasNext(): Boolean = {
            val hasNext = kvsReader.hasNext
            hasNext
          }

          override def next(): Product2[K, C] = {
            val bw = kvsReader.next().asInstanceOf[BytesWritable].copyBytes()
            val value = serializer.deserialize[Tuple2[_, _]](ByteBuffer.wrap(bw))
//            println(value)
            value.asInstanceOf[Product2[K, C]]
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
}