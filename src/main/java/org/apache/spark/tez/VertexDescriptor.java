/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.tez;


/**
 * Collector class which contains contains data required to build Tez Vertex
 */
public class VertexDescriptor {
	private final int stageId;
	private final int vertexId;
	private final Object input;
	private final TezTask<?> tezTask;
	private int numPartitions = 1; // must be set to the amount of reducers or 1. Must NEVER be 0 otherwise there will be ArithmeticException in partitioner
	private String vertexNameIndex;

	/**
	 * 
	 * @param stageId
	 * @param vertexId
	 * @param input
	 * @param serTaskData
	 */
	public VertexDescriptor(int stageId, int vertexId, Object input, TezTask<?> tezTask){
		this.stageId = stageId;
		this.vertexId = vertexId;
		this.input = input;
		this.tezTask = tezTask;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getVertexNameIndex() {
		return vertexNameIndex;
	}

	/**
	 * 
	 * @param vertexNameIndex
	 */
	protected void setVertexNameIndex(String vertexNameIndex) {
		this.vertexNameIndex = vertexNameIndex;
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
	public TezTask<?> getTask() {
		return this.tezTask;
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
	 * @param numPartitions
	 */
	public void setNumPartitions(int numPartitions) {
		if (numPartitions > 0){
			this.numPartitions = numPartitions;
		}
	}
}