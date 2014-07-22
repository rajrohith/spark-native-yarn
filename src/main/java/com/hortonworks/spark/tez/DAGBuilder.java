package com.hortonworks.spark.tez;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.InputDescriptor;
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
import org.springframework.core.io.ClassPathResource;

import com.hortonworks.spark.tez.processor.TezSparkProcessor;
import com.hortonworks.spark.tez.utils.YarnUtils;

public class DAGBuilder {
	
	private final Log logger = LogFactory.getLog(DAGBuilder.class);
	
	private final TezConfiguration tezConfiguration;
	
	private final String applicationName;
	
	private final Map<Integer, VertexDescriptor> vertexes = new LinkedHashMap<Integer, VertexDescriptor>();
	
	private final ApplicationId applicationId;
	
	private final TezClient tezClient;
	
	private final FileSystem fileSystem;
	
	private final String user;
	
	private final Path stagingDir;
	
	private Map<String, LocalResource> localResources;
	
	private final String outputPath;
	
	private String[] classpathExclusions;
	
	private DAG dag;
	
	public DAGBuilder(String applicationName, TezConfiguration tezConfiguration, String outputPath) {
		this.tezConfiguration = tezConfiguration;
		this.applicationName = applicationName;
		this.dag = new DAG(this.applicationName);
		
		this.tezClient = new TezClient("WordCountSession", this.tezConfiguration);
		this.applicationId = createApplicationId(this.tezConfiguration);
		this.fileSystem = createFileSystem(this.tezConfiguration);
		this.initClasspathExclusions();
		
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
	    	this.tezClient.start();

		    DAGClient dagClient = null;
	        if (this.fileSystem.exists(new Path(outputPath))) {
	          throw new FileAlreadyExistsException("Output directory " + this.outputPath + " already exists");
	        }

	        tezClient.waitTillReady();
	        dagClient = tezClient.submitDAGApplication(this.applicationId, this.dag);

	        // monitoring
	        DAGStatus dagStatus = dagClient.waitForCompletionWithAllStatusUpdates(null);
	        if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
	          System.out.println("DAG diagnostics: " + dagStatus.getDiagnostics());
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
		this.provisionAndLocalizeCurrentClasspath();
		this.provisionAndLocalizeScalaLib();
		this.tezClient.addAppMasterLocalResources(this.localResources);
		
		ProcessorDescriptor pd = new ProcessorDescriptor(TezSparkProcessor.class.getName());
		OrderedPartitionedKVEdgeConfigurer edgeConf = OrderedPartitionedKVEdgeConfigurer
		        .newBuilder(BytesWritable.class.getName(), BytesWritable.class.getName(),
		            HashPartitioner.class.getName(), null).build();
		
		int sequenceCounter = 0;
		for (Entry<Integer, VertexDescriptor> vertexDescriptorEntry : vertexes.entrySet()) {
			VertexDescriptor vertexDescriptor = vertexDescriptorEntry.getValue();
			
			if (vertexDescriptor.input instanceof String) {
				Configuration vertexConfig = new Configuration(this.tezConfiguration);
				vertexConfig.set(FileInputFormat.INPUT_DIR, (String) vertexDescriptor.input);
				InputDescriptor id = new InputDescriptor(MRInput.class.getName())
		        .setUserPayload(MRInput.createUserPayload(vertexConfig,
		        		vertexDescriptor.inputFormatClass.getName(), true, true));
				Vertex vertex = new Vertex(String.valueOf(sequenceCounter++), pd, -1, MRHelpers.getMapResource(this.tezConfiguration));
				vertex.addInput(String.valueOf(sequenceCounter++), id, MRInputAMSplitGenerator.class);
				vertex.setTaskLocalFiles(this.localResources);
				vertex.setTaskLaunchCmdOpts(MRHelpers.getMapJavaOpts(this.tezConfiguration));
				this.dag.addVertex(vertex);
			}
			else {
				if (vertexDescriptorEntry.getKey() == 0) {
					Configuration outputConf = new Configuration(this.tezConfiguration);
					outputConf.set(FileOutputFormat.OUTDIR, this.outputPath);
					OutputDescriptor od = new OutputDescriptor(MROutput.class.getName()).setUserPayload(MROutput
							.createUserPayload(outputConf, SequenceFileAsBinaryOutputFormat.class.getName(), true));
					Vertex vertex = new Vertex(String.valueOf(sequenceCounter++), pd, vertexDescriptor.numPartitions, MRHelpers.getReduceResource(this.tezConfiguration));
					vertex.addOutput(String.valueOf(sequenceCounter++), od, MROutputCommitter.class);
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
					// This is temporary until ready to test  DAG with multi-stages
					throw new IllegalStateException("Unrecognized VertexDescriptor. Dev BUG, Fix!!!!");
				}
			}
		}
	}
	
	/**
	 * 
	 */
	private void initClasspathExclusions(){
		try {
			ClassPathResource exclusionResource = new ClassPathResource("classpath_exclusions");
			if (exclusionResource.exists()){
				List<String> exclusionPatterns = new ArrayList<String>();
				File file = exclusionResource.getFile();
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line;
				while ((line = reader.readLine()) != null){
					exclusionPatterns.add(line.trim());
				}
				this.classpathExclusions = exclusionPatterns.toArray(new String[]{});
				reader.close();
			}
			
		} catch (Exception e) {
			logger.warn("Failed to build the list of classpath exclusion. ", e);
		}
	}
	
	/**
	 * 
	 */
	private void provisionAndLocalizeCurrentClasspath() {
		System.out.println(this.tezClient.getAppMasterApplicationId());
		Path[] provisionedResourcesPaths = YarnUtils.provisionClassPath(
				this.fileSystem, this.applicationName, 
				this.applicationId, this.classpathExclusions);
		this.localResources = YarnUtils.createLocalResources(this.fileSystem, 
				provisionedResourcesPaths);
	}
	
	/**
	 * 
	 */
	private void provisionAndLocalizeScalaLib(){
		URL url = ClassLoader.getSystemClassLoader().getResource("scala/Function.class");
		String path = url.getFile();
		path = path.substring(0, path.indexOf("!"));
		
		try {
			File scalaLibLocation = new File(new URL(path).toURI());
			this.provisionAndAddToLocalResources(scalaLibLocation);
		} catch (Exception e) {
			throw new RuntimeException("Failed to provision Scala Library", e);
		}
	}
	
	/**
	 * 
	 * @param file
	 */
	private void provisionAndAddToLocalResources(File file){
		Path provisionedPath = YarnUtils.provisionResource(file, this.fileSystem, 
				this.applicationName, this.applicationId);
		LocalResource localResource = YarnUtils.createLocalResource(this.fileSystem, provisionedPath);
		this.localResources.put(provisionedPath.getName(), localResource);
	}
	
	private static ApplicationId createApplicationId(TezConfiguration tezConfiguration){
		try {
			YarnClient yarnClient = YarnClient.createYarnClient();
			yarnClient.init(tezConfiguration);
			yarnClient.start();
			return yarnClient.createApplication().
			          getNewApplicationResponse().getApplicationId();
		} catch (Exception e) {
			throw new IllegalStateException("Can't generate application id", e);
		}
	}
	
	private static FileSystem createFileSystem(TezConfiguration tezConfiguration){
		try {
			return FileSystem.get(tezConfiguration);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to access FileSystem", e);
		}
	}
}
