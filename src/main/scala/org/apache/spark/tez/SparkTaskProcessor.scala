package org.apache.spark.tez

import org.apache.spark.Logging
import org.apache.tez.mapreduce.processor.SimpleMRProcessor
import org.apache.tez.runtime.api.LogicalInput
import org.apache.tez.runtime.api.LogicalOutput
import org.apache.tez.runtime.api.ProcessorContext

class SparkTaskProcessor(val context: ProcessorContext) extends SimpleMRProcessor(context) with Logging {

  def run() = {
    try {
      this.doRun();
    } catch {
      case e: Exception =>
        e.printStackTrace();
        throw new IllegalStateException("Failed to execute processor for Vertex " + this.getContext().getTaskVertexIndex(), e);
    }
  }

  private def doRun() = {
    logInfo("Executing processor for task: " + this.getContext().getTaskIndex() + " for DAG " + this.getContext().getDAGName());
    val inputs = this.toIntKey(this.getInputs()).asInstanceOf[java.util.Map[Integer, LogicalInput]]
    val outputs = this.toIntKey(this.getOutputs()).asInstanceOf[java.util.Map[Integer, LogicalOutput]]

    ExecutionContext.setObjectRegistry(context.getObjectRegistry());

    val taskBytes = TezUtils.getTaskBuffer(context)
    val vertexTask = SparkUtils.deserializeSparkTask(taskBytes, this.getContext().getTaskIndex());

    val shuffleStage = vertexTask.isInstanceOf[VertexShuffleTask]
    val shufleManager = new TezShuffleManager(inputs, outputs, shuffleStage);
    SparkUtils.createSparkEnv(shufleManager);

    SparkUtils.runTask(vertexTask);
  }

  /**
   * 
   */
  private def toIntKey(map: java.util.Map[String, _]): java.util.Map[Integer, _] = {
    val resultMap = new java.util.TreeMap[Integer, Any]();
    val iter = map.keySet().iterator()
    while (iter.hasNext()) {
      val indexString = iter.next()
      try {
        resultMap.put(Integer.parseInt(indexString), map.get(indexString));
      } catch {
        case e: NumberFormatException =>
          throw new IllegalArgumentException("Vertex name must be parsable to Integer. Was: '" + indexString + "'", e);
      }
    }
    resultMap
  }
}