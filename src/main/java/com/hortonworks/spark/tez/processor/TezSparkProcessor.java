package com.hortonworks.spark.tez.processor;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkUtils;
import org.apache.spark.TezShuffleManager;
import org.apache.spark.scheduler.Task;
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
		
//		int vertextId = this.context.getTaskVertexIndex();
//		String serializedTaskName = "SparkTask_" + vertextId + ".ser";
//		ClassPathResource serializedTask = new ClassPathResource(serializedTaskName, Thread.currentThread().getContextClassLoader());
//		Assert.isTrue(serializedTask.exists(), "Can't locate serialized task '" + serializedTaskName + "' on the classpath");
		
		Map<Integer, LogicalInput> inputs = (Map<Integer, LogicalInput>)this.toIntKey(this.getInputs());
		Map<Integer, LogicalOutput> outputs = (Map<Integer, LogicalOutput>)this.toIntKey(this.getOutputs());
		TezShuffleManager shufleManager = new TezShuffleManager(inputs, outputs);
		SparkUtils.createSparkEnv(shufleManager);
		
		ByteArrayInputStream serializedTask = new ByteArrayInputStream(this.context.getUserPayload());
		
		Task<?> vertexTask = SparkUtils.deserializeSparkTask(serializedTask, this.context.getTaskIndex());
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