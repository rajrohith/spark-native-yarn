package com.hortonworks.spark.tez.processor;

import java.nio.ByteBuffer;
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
import org.apache.tez.runtime.api.ProcessorContext;

/**
 * 
 */
public class TezSparkProcessor extends SimpleMRProcessor {
	
	private final Log logger = LogFactory.getLog(TezSparkProcessor.class);
	
	/**
	 * 
	 * @param context
	 */
	public TezSparkProcessor(ProcessorContext context) {
		super(context);
	}
	
	/**
	 * 
	 */
	@Override
	public void run() throws Exception {
		try {
			this.doRun();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException("Failed to execute processor for Vertex " + this.getContext().getTaskVertexIndex(), e);
		}
	}
	
	/**
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private void doRun() throws Exception {
		if (logger.isInfoEnabled()){
			logger.info("Executing processor for task: " + this.getContext().getTaskIndex() + " for DAG " + this.getContext().getDAGName());
		}

		Map<Integer, LogicalInput> inputs = (Map<Integer, LogicalInput>)this.toIntKey(this.getInputs());
		Map<Integer, LogicalOutput> outputs = (Map<Integer, LogicalOutput>)this.toIntKey(this.getOutputs());
		TezShuffleManager shufleManager = new TezShuffleManager(inputs, outputs);
		SparkUtils.createSparkEnv(shufleManager);
		
		ByteBuffer payload = this.getContext().getUserPayload().getPayload();
		payload.rewind();
		byte[] pBytes = new byte[payload.capacity()];
		payload.get(pBytes);
		VertexTask vertexTask = SparkUtils.deserializeSparkTask(pBytes, this.getContext().getTaskIndex());
		SparkUtils.runTask(vertexTask);
	}
	
	/**
	 * 
	 * @param map
	 * @return
	 */
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