package org.apache.spark.tez

import org.apache.spark.scheduler.Task
import org.apache.spark.Partitioner
import org.apache.spark.Logging

private[tez] abstract class TezTask[U](stageId: Int, partitionId: Int) extends Task[U](stageId, 0) with Logging {

  var partitioner:Partitioner = null
  
  def setPartitioner(partitioner:Partitioner) {
    this.partitioner = partitioner
  }
}