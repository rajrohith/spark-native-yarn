package org.apache.spark.tez;

import java.nio.ByteBuffer;

/**
 * Collector class which contains contains data required to build Tez Vertex
 */
public class VertexDescriptor {
	private final int stageId;
	private final int vertexId;
	private final Object input;
	private final ByteBuffer serTaskData;
	private Class<?> inputFormatClass;	
	private Class<?> key;
	private Class<?> value;
	private int numPartitions = 1; // must be set to the amount of reducers or 1. Must NEVER be 0 otherwise there will be ArithmeticException in partitioner
	
	/**
	 * 
	 * @param stageId
	 * @param vertexId
	 * @param input
	 * @param serTaskData
	 */
	public VertexDescriptor(int stageId, int vertexId, Object input, ByteBuffer serTaskData){
		this.stageId = stageId;
		this.vertexId = vertexId;
		this.input = input;
		this.serTaskData = serTaskData;
	}
	
	/**
	 * 
	 * @return
	 */
	public int getStageId() {
		return stageId;
	}

	/**
	 * 
	 * @return
	 */
	public int getVertexId() {
		return vertexId;
	}

	/**
	 * 
	 * @return
	 */
	public Object getInput() {
		return input;
	}

	/**
	 * 
	 * @return
	 */
	public ByteBuffer getSerTaskData() {
		return serTaskData;
	}

	/**
	 * 
	 * @return
	 */
	public Class<?> getInputFormatClass() {
		return inputFormatClass;
	}

	/**
	 * 
	 * @return
	 */
	public Class<?> getKey() {
		return key;
	}

	/**
	 * 
	 * @return
	 */
	public Class<?> getValue() {
		return value;
	}

	/**
	 * 
	 * @return
	 */
	public int getNumPartitions() {
		return numPartitions;
	}
	
	/**
	 * 
	 */
	public String toString(){
		return "(stage: " + this.stageId + "; vertex:" + this.vertexId + "; input:" + input + ")";
	}
	
	/**
	 * 
	 * @param inputFormatClass
	 */
	public void setInputFormatClass(Class<?> inputFormatClass) {
		this.inputFormatClass = inputFormatClass;
	}

	/**
	 * 
	 * @param key
	 */
	public void setKey(Class<?> key) {
		this.key = key;
	}

	/**
	 * 
	 * @param value
	 */
	public void setValue(Class<?> value) {
		this.value = value;
	}
	
	/**
	 * 
	 * @param numPartitions
	 */
	public void setNumPartitions(int numPartitions) {
		if (numPartitions > 0){
			this.numPartitions = numPartitions;
		}
	}
}