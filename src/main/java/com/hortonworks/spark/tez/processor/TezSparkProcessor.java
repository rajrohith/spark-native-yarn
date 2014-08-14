package com.hortonworks.spark.tez.processor;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkUtils;
import org.apache.spark.TezShuffleManager;
import org.apache.spark.tez.VertexTask;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;

public class TezSparkProcessor extends SimpleMRProcessor {
	
	private final Log logger = LogFactory.getLog(TezSparkProcessor.class);
	
	@Override
	public void run() throws Exception {
		try {
			this.doRun();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException("Failed to execute processor for Vertex " + this.context.getTaskVertexIndex(), e);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void doRun() throws Exception {
		if (logger.isInfoEnabled()){
			logger.info("Executing processor for task: " + this.context.getTaskIndex() + " for DAG " + this.context.getDAGName());
		}
		String dataName = this.context.getDAGName() + "_p_" + this.context.getTaskIndex();
		
		//System.out.println("Vertex Index: " + this.context.get + "-" + this.context.getTaskIndex());

		Map<Integer, LogicalInput> inputs = (Map<Integer, LogicalInput>)this.toIntKey(this.getInputs());
		Map<Integer, LogicalOutput> outputs = (Map<Integer, LogicalOutput>)this.toIntKey(this.getOutputs());
		TezShuffleManager shufleManager = new TezShuffleManager(inputs, outputs);
		SparkUtils.createSparkEnv(shufleManager);
		
		VertexTask vertexTask = SparkUtils.deserializeSparkTask(this.context.getUserPayload(), this.context.getTaskIndex());
		SparkUtils.runTask(vertexTask);
	}
	
	private Map<Integer, ?> toIntKey(Map<String, ?> map) {
		TreeMap<Integer, Object> resultMap = new TreeMap<Integer, Object>();
		for (String indexName : map.keySet()) {
			try {
				resultMap.put(Integer.parseInt(indexName), map.get(indexName));
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Vertex name must be parsable to Integer. Was: '" + indexName + "'", e);
			}
		}
		return resultMap;
    }
}