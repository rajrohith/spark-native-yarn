package com.hortonworks.spark.tez;

import java.io.File;
import java.io.FileInputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.OutputCommitterDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.committer.MROutputCommitter;
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfigurer;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

import com.hortonworks.spark.tez.processor.TezSparkProcessor;
import com.hortonworks.spark.tez.utils.YarnUtils;

public class DAGBuilder {
	
	private final Log logger = LogFactory.getLog(DAGBuilder.class);
	
	private final TezConfiguration tezConfiguration;
	
	private final Map<Integer, VertexDescriptor> vertexes = new LinkedHashMap<Integer, VertexDescriptor>();
	
	private final TezClient tezClient;
	
	private final FileSystem fileSystem;
	
	private final String user;
	
	private final Path stagingDir;
	
	private final Map<String, LocalResource> localResources;
	
	private final String outputPath;

	private DAG dag;
	
	public DAGBuilder(TezClient tezClient, Map<String, LocalResource> localResources, TezConfiguration tezConfiguration, String outputPath) {
		this.tezClient = tezClient;
		this.tezConfiguration = tezConfiguration;
		this.dag = new DAG(tezClient.getClientName() + "_" + System.currentTimeMillis());
		this.fileSystem = YarnUtils.createFileSystem(this.tezConfiguration);

		try {
			this.user = UserGroupInformation.getCurrentUser().getShortUserName();
		} catch (Exception e) {
			throw new IllegalStateException("Failed to get current user", e);
		}
		String stagingDirStr = Path.SEPARATOR + "user" + Path.SEPARATOR + user + Path.SEPARATOR+ ".staging" + Path.SEPARATOR
		        + Path.SEPARATOR + Long.toString(System.currentTimeMillis());
		this.tezConfiguration.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
		this.stagingDir = this.fileSystem.makeQualified(new Path(stagingDirStr));
		this.outputPath = outputPath;
		this.localResources = localResources;
	}
	
	public DAGExecutor build(){
		return new DAGExecutor(){
			@Override
			public void execute() {
				DAGBuilder.this.run();
			}
		};
	}
		
	private void run(){
	    try { 	
	    	this.doBuild();

		    DAGClient dagClient = null;
//	        if (this.fileSystem.exists(new Path(outputPath))) {
//	          throw new FileAlreadyExistsException("Output directory " + this.outputPath + " already exists");
//	        }

	        tezClient.waitTillReady();
	        dagClient = tezClient.submitDAG(this.dag);

	        // monitoring
	        DAGStatus dagStatus = dagClient.waitForCompletionWithAllStatusUpdates(null);
	        if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
	          logger.error("DAG diagnostics: " + dagStatus.getDiagnostics());
	        }
	    } catch (Exception e) {
	    	throw new IllegalStateException("Failed to execute DAG", e);
	    } finally {
	    	try {
				this.fileSystem.delete(this.stagingDir, true);
			} catch (Exception e) {
				// ignore
			}
	    }
	}

	public void addVertex(VertexDescriptor vd) {
		vertexes.put(vd.stageId, vd);
	}
	
	public String toString() {
		return vertexes.toString();
	}
	
	/**
	 * 
	 */
	public interface DAGExecutor {
		void execute();
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unused")
	public static class VertexDescriptor {
		private final int stageId;
		private final int vertexId;
		private final Object input;
		private Class<?> inputFormatClass;	
		private Class<?> key;
		private Class<?> value;
		private int numPartitions;
		
		public VertexDescriptor(int stageId, int vertexId, Object input){
			this.stageId = stageId;
			this.vertexId = vertexId;
			this.input = input;
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
			this.numPartitions = numPartitions;
		}
	}
	
	@SuppressWarnings("unchecked")
	private void doBuild() throws Exception {
		logger.debug("Building Tez DAG");
		File serTaskDir = new File(System.getProperty("java.io.tmpdir") + "/" + this.tezClient.getClientName());

		OrderedPartitionedKVEdgeConfigurer edgeConf = OrderedPartitionedKVEdgeConfigurer
		        .newBuilder(BytesWritable.class.getName(), BytesWritable.class.getName(),
		            HashPartitioner.class.getName(), null).build();
		
		int sequenceCounter = 0;
		int counter = 0;
		for (Entry<Integer, VertexDescriptor> vertexDescriptorEntry : vertexes.entrySet()) {
			counter++;
			VertexDescriptor vertexDescriptor = vertexDescriptorEntry.getValue();
			
			if (vertexDescriptor.input instanceof String) {
				ProcessorDescriptor pd = new ProcessorDescriptor(TezSparkProcessor.class.getName());
				
				File taskFile = new File(serTaskDir, "SparkTask_" + vertexDescriptor.vertexId + ".ser");
				byte[] taskBytes = new byte[(int)taskFile.length()];
				IOUtils.readFully(new FileInputStream(taskFile), taskBytes);
				pd.setUserPayload(taskBytes);
				
				Configuration vertexConfig = new Configuration(this.tezConfiguration);
				vertexConfig.set(FileInputFormat.INPUT_DIR, (String) vertexDescriptor.input);
				byte[] payload = MRInput.createUserPayload(vertexConfig,vertexDescriptor.inputFormatClass.getName(), true, true);
				InputDescriptor id = new InputDescriptor(MRInput.class.getName()).setUserPayload(payload);
				Vertex vertex = new Vertex(String.valueOf(sequenceCounter++), pd, -1, MRHelpers.getMapResource(this.tezConfiguration));
				vertex.addDataSource(String.valueOf(sequenceCounter++), id, new InputInitializerDescriptor(MRInputAMSplitGenerator.class.getName()));
				vertex.setTaskLocalFiles(this.localResources);
				vertex.setTaskLaunchCmdOpts(MRHelpers.getMapJavaOpts(this.tezConfiguration));
				this.dag.addVertex(vertex);
			}
			else {
//				if (vertexDescriptorEntry.getKey() == 0) {
				if (counter == vertexes.size()) {
					ProcessorDescriptor pd = new ProcessorDescriptor(TezSparkProcessor.class.getName());
					
					File taskFile = new File(serTaskDir, "SparkTask_" + vertexDescriptor.vertexId + ".ser");
					byte[] taskBytes = new byte[(int)taskFile.length()];
					IOUtils.readFully(new FileInputStream(taskFile), taskBytes);
					pd.setUserPayload(taskBytes);
					
					Configuration outputConf = new Configuration(this.tezConfiguration);
					outputConf.set(FileOutputFormat.OUTDIR, this.outputPath);
					OutputDescriptor od = new OutputDescriptor(MROutput.class.getName()).setUserPayload(MROutput
							.createUserPayload(outputConf, SequenceFileAsBinaryOutputFormat.class.getName(), true));
					Vertex vertex = new Vertex(String.valueOf(sequenceCounter++), pd, vertexDescriptor.numPartitions, MRHelpers.getReduceResource(this.tezConfiguration));
					vertex.addDataSink(String.valueOf(sequenceCounter++), od, new OutputCommitterDescriptor(MROutputCommitter.class.getName()) );
					vertex.setTaskLocalFiles(localResources);
				    vertex.setTaskLaunchCmdOpts(MRHelpers.getMapJavaOpts(this.tezConfiguration));
				    this.dag.addVertex(vertex);
				    if (!(vertexDescriptor.input instanceof String)) {
				    	for (int stageId : (Iterable<Integer>)vertexDescriptor.input) {
				    		VertexDescriptor vd = vertexes.get(stageId);
					    	String vertexName =  vd.vertexId * 2 + "";
					    	Vertex v = dag.getVertex(vertexName);
					    	Edge edge = new Edge(v, vertex, edgeConf.createDefaultEdgeProperty());
					    	this.dag.addEdge(edge);
						}
				    }    
				}
				else {
					ProcessorDescriptor pd = new ProcessorDescriptor(TezSparkProcessor.class.getName());
					
					File taskFile = new File(serTaskDir, "SparkTask_" + vertexDescriptor.vertexId + ".ser");
					byte[] taskBytes = new byte[(int)taskFile.length()];
					IOUtils.readFully(new FileInputStream(taskFile), taskBytes);
					pd.setUserPayload(taskBytes);
					
					Vertex vertex = new Vertex(String.valueOf(sequenceCounter++), pd, vertexDescriptor.numPartitions, MRHelpers.getReduceResource(this.tezConfiguration));
					
					vertex.setTaskLocalFiles(localResources);
				    vertex.setTaskLaunchCmdOpts(MRHelpers.getMapJavaOpts(this.tezConfiguration));
				    this.dag.addVertex(vertex);
				    if (!(vertexDescriptor.input instanceof String)) {
				    	for (int stageId : (Iterable<Integer>)vertexDescriptor.input) {
				    		VertexDescriptor vd = vertexes.get(stageId);
					    	String vertexName =  vd.vertexId * 2 + "";
					    	Vertex v = dag.getVertex(vertexName);
					    	Edge edge = new Edge(v, vertex, edgeConf.createDefaultEdgeProperty());
					    	this.dag.addEdge(edge);
						}
				    } 
				}
			}
		}
		logger.debug("Finished building Tez DAG");
	}
	
}
