package org.apache.spark.tez

import org.apache.spark.rdd.RDD
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.Partitioner
import org.apache.spark.SparkEnv
import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.CoGroupPartition
import org.apache.spark.rdd.ShuffledRDD
import scala.reflect.ClassTag
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.CoGroupedRDD
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.NarrowCoGroupSplitDep
import org.apache.spark.rdd.ShuffleCoGroupSplitDep
import org.apache.spark.util.collection.AppendOnlyMap
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.InterruptibleIterator
import org.apache.hadoop.io.BytesWritable
import java.nio.ByteBuffer
import org.apache.spark.serializer.Serializer

class TezPairRDDFunctions[K, V](self: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null) {

//  def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Iterable[V], Iterable[W]))] = {
//    println("$$$$$$$$$$$$$$$$")
//    val s = this.asInstanceOf[PairRDDFunctions[K, V]]
//    val f = s.getClass().getDeclaredFields()
//    var selfField: RDD[_ <: Product2[K, _]] = null
//    for (field <- f) {
//      if (field.getName().endsWith("self")) {
//        field.setAccessible(true)
//        selfField = field.get(this).asInstanceOf[RDD[_ <: Product2[K, _]]]
//        println("SET SELF")
//      }
//    }
//    println("SELF: " + s)
////    val cg = new CoGroupedRDD[K](Seq(selfField, other), partitioner) with CoGroupedRDDMixin[K]
//    val cg = new TezCoGroupedRDD[K](Seq(selfField, other), partitioner)
//    //    val cg = new MyRDD[Any](other.context)
//    cg.mapValues {
//      case Array(vs, w1s) =>
//        (vs.asInstanceOf[Iterable[V]], w1s.asInstanceOf[Iterable[W]])
//    }
//  }
  def combineByKey[C](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null): RDD[(K, C)] = {
    println("############### BUSTED")
    null
//    this.asInstanceOf[PairRDDFunctions[K,V]].combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine, serializer)
  }
}

//trait CoGroupedRDDMixin[K] extends CoGroupedRDD[K] {
//  override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, Array[Iterable[_]])] = {
//    println("######## COMPUTINF")
//    val split = theSplit.asInstanceOf[CoGroupPartition]
//    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, _, _]]
//    val iterator = SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
//      .read()
//      .asInstanceOf[Iterator[(K, Array[Iterable[_]])]]
//    iterator
//  }
//}
class TezCoGroupedRDD[K](@transient rdds: Seq[RDD[_ <: Product2[K, _]]], part: Partitioner)
  //extends RDD[(K, Array[Iterable[_]])](rdds.head.context, Nil) {
	extends CoGroupedRDD[K](rdds, part) {
  
  private type CoGroup = CompactBuffer[Any]
  private type CoGroupCombiner = Array[CoGroup]
  
//  println("foo")

//  override def getPartitions: Array[Partition] = {
//    Array(new Partition {
//      override def index: Int = 0
//    })
//  }
  /**
   *
   */
 // override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, Array[Iterable[_]])] = {
  override def compute(s: Partition, context: TaskContext): Iterator[(K, Array[Iterable[_]])] = {
    println("######## COMPUTINF")
    
//    val split = theSplit.asInstanceOf[CoGroupPartition]
//    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, _, _]]
//    val iterator = SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
//      .read()
//      .asInstanceOf[Iterator[(K, Array[Iterable[_]])]]
//    iterator
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = split.deps.size
    val rddIterators = new ArrayBuffer[(Iterator[Product2[K, Any]], Int)]
    for ((dep, depNum) <- split.deps.zipWithIndex) dep match {
      case NarrowCoGroupSplitDep(rdd, _, itsSplit) =>
        // Read them from the parent
        val it = rdd.iterator(itsSplit, context).asInstanceOf[Iterator[Product2[K, Any]]]
        rddIterators += ((it, depNum))

      case ShuffleCoGroupSplitDep(handle) =>
        // Read map outputs of shuffle
        val it = SparkEnv.get.shuffleManager
          .getReader(handle, split.index, split.index + 1, context)
          .read()
        rddIterators += ((it, depNum))
    }
    
    val map = new AppendOnlyMap[K, CoGroupCombiner]
    val update: (Boolean, CoGroupCombiner) => CoGroupCombiner = (hadVal, oldVal) => {
      if (hadVal) oldVal else Array.fill(numRdds)(new CoGroup)
    }
    val getCombiner: K => CoGroupCombiner = key => {
      println("=======> getCombiner")
      map.changeValue(key, update)
    }
    
    val serializer = SparkEnv.get.serializer.newInstance
    
    rddIterators.foreach {
      case (it, depNum) =>
        while (it.hasNext) {
          val kv = it.next
          val iter = kv._2.asInstanceOf[java.lang.Iterable[_]].iterator()
          while (iter.hasNext()){
            val value = iter.next()
            val dValue = serializer.deserialize[Any](ByteBuffer.wrap(value.asInstanceOf[BytesWritable].getBytes()))
            println(kv._1 + "-" + dValue)
            getCombiner(kv._1)(depNum) += dValue
//            map.changeValue(kv._1, dValue)
          } 
        }
    }
    
    val mIter = map.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]]
    val iterator = new InterruptibleIterator(context, mIter)
    //    val iterator = rddIterators.toIterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]]
//    while (iterator.hasNext){
//      val next = iterator.next
//      println(next)
//    }
    iterator
  }
}
