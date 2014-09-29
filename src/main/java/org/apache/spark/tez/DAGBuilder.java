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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.spark.tez.io.TezRDD;
//import org.apache.spark.tez.io.ValueWritable;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

/**
 * Builder which builds Tez DAG based on the collection of {@link VertexDescriptor}
 * added to it via {@link #addVertex(VertexDescriptor)}.
 * 
 * Once all Vertexes were added invoking {@link #build()} method will generate an executable
 * {@link DAGTask} (See {@link DAGTask#execute()} method)
 * 
 * The class is package private. Its main gateway is {@link Utils}.
 */
class DAGBuilder {
	
	private final Log logger = LogFactory.getLog(DAGBuilder.class);
	
	private final Configuration tezConfiguration;
	
	private final Map<Integer, VertexDescriptor> vertexes = new LinkedHashMap<Integer, VertexDescriptor>();
	
	private final TezClient tezClient;

	private final Map<String, LocalResource> localResources;
	
	private final String applicationInstanceName;

	private DAG dag;
	
	/**
	 * 
	 * @param tezClient
	 * @param localResources
	 * @param tezConfiguration
	 * @param outputPath
	 */
	public DAGBuilder(TezClient tezClient, Map<String, LocalResource> localResources, Configuration tezConfiguration) {
		this.tezClient = tezClient;
		this.tezConfiguration = tezConfiguration;
		this.applicationInstanceName = tezClient.getClientName() + "_" + System.currentTimeMillis();
		this.dag = DAG.create(this.applicationInstanceName);
		this.localResources = localResources;
	}
	/**
	 * 
	 * @return
	 */
	public DAGTask build(Class<?> keyClass, Class<?> valueClass, Class<?> outputFormatClass, String outputPath){
		try {
			this.doBuild(keyClass, valueClass, outputFormatClass, outputPath);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return new DAGTask(){
			@Override
			public void execute() {
				try {
					DAGBuilder.this.run();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					try {
						tezClient.stop();
					} catch (Exception e2) {
						// ignore
					}
				}
			}
		};
	}
	
	/**
	 * 
	 */
	private void run(){
	    try { 	
		    DAGClient dagClient = null;
//	        if (this.fileSystem.exists(new Path(outputPath))) {
//	          throw new FileAlreadyExistsException("Output directory " + this.outputPath + " already exists");
//	        }

	        tezClient.waitTillReady();
	       
	        if (logger.isInfoEnabled()){
	        	logger.info("Submitting generated DAG to YARN/Tez cluster");
	        }
	 
	        dagClient = tezClient.submitDAG(this.dag);

	
	        DAGStatus dagStatus =  dagClient.waitForCompletionWithStatusUpdates(null);
	        
	        if (logger.isInfoEnabled()){
	        	logger.info("DAG execution complete");
	        }
	        if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
	          logger.error("DAG diagnostics: " + dagStatus.getDiagnostics());
	        }
	    } catch (Exception e) {
	    	throw new IllegalStateException("Failed to execute DAG", e);
	    } 
	}

	/**
	 * 
	 * @param vd
	 */
	public void addVertex(VertexDescriptor vd) {
		vertexes.put(vd.getStageId(), vd);
	}
	
	/**
	 * 
	 */
	public String toString() {
		return vertexes.toString();
	}
	/**
	 * 
	 * @throws Exception
	 */
	private void doBuild(Class<?> keyClass, Class<?> valueClass, Class<?> outputFormatClass, String outputPath) throws Exception {
		if (logger.isDebugEnabled()){
			logger.debug("Building Tez DAG");
		}

		OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
		        .newBuilder(keyClass.getName(), "org.apache.spark.tez.io.ValueWritable", HashPartitioner.class.getName(), null).build();

		int sequenceCounter = 0;
		int counter = 0;
		for (Entry<Integer, VertexDescriptor> vertexDescriptorEntry : vertexes.entrySet()) {
			counter++;
			VertexDescriptor vertexDescriptor = vertexDescriptorEntry.getValue();
			
			if (vertexDescriptor.getInput() instanceof TezRDD) {
				String inputPath = ((TezRDD<?,?>)vertexDescriptor.getInput()).getPath();
				Class<?> inputFormatClass = ((TezRDD<?,?>)vertexDescriptor.getInput()).inputFormatClass();
				DataSourceDescriptor dataSource = MRInput.createConfigBuilder(new Configuration(tezConfiguration), inputFormatClass, inputPath).build();		
				UserPayload payload = UserPayload.create(vertexDescriptor.getSerTaskData());
				String vertexName = String.valueOf(sequenceCounter++);
				String dsName = String.valueOf(sequenceCounter++);
				Vertex vertex = Vertex.create(vertexName, ProcessorDescriptor.create(SparkTaskProcessor.class.getName()).setUserPayload(payload))
						.addDataSource(dsName, dataSource);	
				// For single stage vertex we need to add data sink
				if (counter == vertexes.size()){
					JobConf dsConfig = new JobConf(tezConfiguration);
					dsConfig.setOutputKeyClass(keyClass);
					dsConfig.setOutputValueClass(valueClass);
					DataSinkDescriptor dataSink = MROutput.createConfigBuilder(dsConfig, outputFormatClass, outputPath).build();
					vertex.addDataSink(dsName, dataSink);
				}
				
				vertex.addTaskLocalFiles(localResources);			
				dag.addVertex(vertex);
			}
			else {
				if (counter == vertexes.size()) {
					JobConf dsConfig = new JobConf(tezConfiguration);
					dsConfig.setOutputKeyClass(keyClass);
					dsConfig.setOutputValueClass(valueClass);
					DataSinkDescriptor dataSink = MROutput.createConfigBuilder(dsConfig, outputFormatClass, outputPath).build();
					UserPayload payload = UserPayload.create(vertexDescriptor.getSerTaskData());
					String vertexName = String.valueOf(sequenceCounter++);
					String dsName = String.valueOf(sequenceCounter++);
					Vertex vertex = Vertex.create(vertexName, ProcessorDescriptor.create(SparkTaskProcessor.class.getName()).setUserPayload(payload), 
							vertexDescriptor.getNumPartitions()).addDataSink(dsName, dataSink);
					vertex.addTaskLocalFiles(localResources);
					
					dag.addVertex(vertex);
					this.addEdges(vertexDescriptor, vertex, edgeConf);   
				}
				else {
					ProcessorDescriptor pd = ProcessorDescriptor.create(SparkTaskProcessor.class.getName());
					UserPayload payload = UserPayload.create(vertexDescriptor.getSerTaskData());
					pd.setUserPayload(payload);
					Vertex vertex = Vertex.create(String.valueOf(sequenceCounter++), pd, vertexDescriptor.getNumPartitions(), MRHelpers.getResourceForMRReducer(this.tezConfiguration));
					vertex.addTaskLocalFiles(localResources);
					
				    this.dag.addVertex(vertex);
				    this.addEdges(vertexDescriptor, vertex, edgeConf);
				}
			}
		}
		if (logger.isDebugEnabled()){
			logger.debug("Finished building Tez DAG");
		}
	}
	
	/**
	 * 
	 * @param vertexDescriptor
	 * @param targetVertex
	 * @param edgeConf
	 */
	@SuppressWarnings("unchecked")
	private void addEdges(VertexDescriptor vertexDescriptor, Vertex targetVertex, OrderedPartitionedKVEdgeConfig edgeConf){
		if (!(vertexDescriptor.getInput() instanceof String)) {
	    	for (int stageId : (Iterable<Integer>)vertexDescriptor.getInput()) {
	    		VertexDescriptor vd = vertexes.get(stageId);
		    	String vertexName =  vd.getVertexId() * 2 + "";
		    	Vertex v = dag.getVertex(vertexName);
		    	Edge edge = Edge.create(v, targetVertex, edgeConf.createDefaultEdgeProperty());
		    	this.dag.addEdge(edge);
			}
	    } 
	}
}
