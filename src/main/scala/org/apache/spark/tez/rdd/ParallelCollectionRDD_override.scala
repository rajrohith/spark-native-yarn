package org.apache.spark.tez.rdd

import scala.reflect.ClassTag

import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import scala.collection.Map

class ParallelCollectionRDD_override[T: ClassTag](
    @transient sc: SparkContext,
    @transient data: Seq[T],
    numSlices: Int,
    locationPrefs: Map[Int, Seq[String]])
    extends RDD[T](sc, Nil) {
  
  println("CREATING INSTANCE OF ParallelCollectionRDD")
  override def getPartitions: Array[Partition] = {
    println("Busted when getting partitions")
    Array()
//    val slices = ParallelCollectionRDD.slice(data, numSlices).toArray
//    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
  }

  override def compute(s: Partition, context: TaskContext):Iterator[T] = {
//    new InterruptibleIterator(context, s.asInstanceOf[ParallelCollectionPartition[T]].iterator)
    null
  }

//  override def getPreferredLocations(s: Partition): Seq[String] = {
////    locationPrefs.getOrElse(s.index, Nil)
//    null
//  }
}