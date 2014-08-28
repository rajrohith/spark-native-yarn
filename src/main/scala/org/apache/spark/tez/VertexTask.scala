package org.apache.spark.tez

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.ShuffleDependency

/**
 * Tez vertex Task modeled after Spark's ShufleMapTask
 */
class VertexTask(val rdd:RDD[_], val dep: Option[ShuffleDependency[Any, Any, Any]]) extends Serializable {

  def runTask():Any = {
    var writer: ShuffleWriter[Any, Any] = null
    try {
      val manager = SparkEnv.get.shuffleManager
      val partition = null
      writer = manager.getWriter[Any, Any](null, 1, null)
      
      // original Spark's shuffle code
//      val records = rdd.iterator(partition, null).asInstanceOf[Iterator[_ <: Product2[Any, Any]]]
//
//      val iter =
//        if (dep.get.aggregator.isDefined) {
//          if (dep.get.mapSideCombine) {
//            dep.get.aggregator.get.combineValuesByKey(records, null) //context
//          } else {
//            records
//          }
//        } 
//        else {
//          records
//        }
//      
//      for (elem <- iter){
//        writer.write(iter)
//      }

      writer.write(rdd.iterator(partition, null).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])

      return writer.stop(success = true).get
    } catch {
      case e: Exception =>
        if (writer != null) {
          writer.stop(success = false)
        }
        throw e
    } 
  }
}