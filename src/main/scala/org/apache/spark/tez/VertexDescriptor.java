package org.apache.spark.tez;

import java.nio.ByteBuffer;

public class VertexDescriptor {
	private final int stageId;
	private final int vertexId;
	private final Object input;
	private final ByteBuffer serTaskData;
	private Class<?> inputFormatClass;	
	private Class<?> key;
	private Class<?> value;
	private int numPartitions = 1; // must be set to the amount of reducers or 1. Must NEVER be 0 otherwise there will be ArithmeticException in partitioner
	
	public VertexDescriptor(int stageId, int vertexId, Object input, ByteBuffer serTaskData){
		this.stageId = stageId;
		this.vertexId = vertexId;
		this.input = input;
		this.serTaskData = serTaskData;
	}
	
	public int getStageId() {
		return stageId;
	}

	public int getVertexId() {
		return vertexId;
	}

	public Object getInput() {
		return input;
	}

	public ByteBuffer getSerTaskData() {
		return serTaskData;
	}

	public Class<?> getInputFormatClass() {
		return inputFormatClass;
	}

	public Class<?> getKey() {
		return key;
	}

	public Class<?> getValue() {
		return value;
	}

	public int getNumPartitions() {
		return numPartitions;
	}
	
	public String toString(){
		return "(stage: " + this.stageId + "; vertex:" + this.vertexId + "; input:" + input + ")";
	}
	
	public void setInputFormatClass(Class<?> inputFormatClass) {
		this.inputFormatClass = inputFormatClass;
	}

	public void setKey(Class<?> key) {
		this.key = key;
	}

	public void setValue(Class<?> value) {
		this.value = value;
	}
	
	public void setNumPartitions(int numPartitions) {
		if (numPartitions > 0){
			this.numPartitions = numPartitions;
		}
	}
}