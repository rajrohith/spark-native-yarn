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
import com.hortonworks.spark.tez.KeyValueReaderWrapper
import org.apache.tez.runtime.api.Writer
import java.util.Map
import sun.rmi.log.LogInputStream
import org.apache.tez.runtime.api.LogicalInput
import org.apache.tez.runtime.api.LogicalOutput
import org.apache.tez.runtime.library.api.KeyValueReader
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import java.lang.Long
import java.lang.Iterable
import scala.collection.JavaConverters._
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.IntWritable
import org.apache.spark.shuffle.BaseShuffleHandle

class TezShuffleManager(val input:Map[Integer, LogicalInput], val output:Map[Integer, LogicalOutput]) extends ShuffleManager {
  println("Creating Tez ShuffleManager")
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
//    println("getWriter")
    val serializer = SparkEnv.get.serializer.newInstance
    if (output.size() > 1){
      throw new UnsupportedOperationException("Multiple outputs are not supported yet.")
    }
//    val kvWriter = TezThreadLocalContext.getWriter.asInstanceOf[KeyValueWriter]
    val kvWriter = output.values.iterator.next.getWriter.asInstanceOf[KeyValueWriter]
    val shuffleWriter = new ShuffleWriter[K, V] {
      /** Write a record to this task's output */
      
      val k = new Text
      val v = new IntWritable
      
      def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
//      def write(record: Product2[K, V]): Unit = {
        for (record <- records) {
//          println(record)
//          val bwValue = new BytesWritable(serializer.serialize(record).array())
//          kvWriter.write(key, bwValue)
          
          k.set(record._1.asInstanceOf[String])
          v.set(record._2.asInstanceOf[Integer])
          kvWriter.write(k, v)
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
//    val serializer = SparkEnv.get.serializer.newInstance

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
            
            
            var previousValue:Any = null
            if (readerValue.isInstanceOf[Iterable[_]]){
              val iter = readerValue.asInstanceOf[Iterable[Writable]].asScala  
              val mergeFunction = handle.asInstanceOf[BaseShuffleHandle[K, Any, C]].dependency.aggregator.get.mergeValue
            
              var acumulatedValue:Any = null
              
              for (value <- iter){
                val decodedValue = WritableDecoder.getValue(value)        									
                acumulatedValue = mergeFunction(acumulatedValue.asInstanceOf[C], decodedValue.asInstanceOf[K])
              }
              previousValue = acumulatedValue
            }
            else {
              previousValue = readerValue
            }
 
            val product = (key.toString, previousValue)
            println("Merged: " + product)
            product.asInstanceOf[Product2[K, C]]
            
            
//            val next =
//              if (kvsReader.isSingleValue()) {
//                val key = kvsReader.nextKey
//                val value = kvsReader.nextValue();
//                (key, value).asInstanceOf[Product2[K, C]]
//              } else {
//            	  val key = kvsReader.nextKey
//            	  val value = kvsReader.nextValue
//            	  val product = (key.toString(), value.toString())
//            	  product.asInstanceOf[Product2[K, C]]
////            	  null
//                
////            	  val bwValue = kvsReader.nextValue.asInstanceOf[BytesWritable].copyBytes()
////            	  val value = serializer.deserialize[Tuple2[_, _]](ByteBuffer.wrap(bwValue))
////            	  value.asInstanceOf[Product2[K, C]]
//              }
//            next
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
object WritableDecoder {
  
  def getValue(writable:Writable) = {
    if (writable.isInstanceOf[LongWritable]){
      writable.asInstanceOf[LongWritable].get
    } else if (writable.isInstanceOf[IntWritable]){
      writable.asInstanceOf[IntWritable].get
    } else if (writable.isInstanceOf[Text]){
      writable.toString
    } else {
      throw new IllegalStateException("Unsupported writable " + writable)
    }
  }
  

}