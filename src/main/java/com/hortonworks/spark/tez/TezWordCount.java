package com.hortonworks.spark.tez;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.Output;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfigurer;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

import com.google.common.base.Preconditions;
import com.hortonworks.spark.tez.utils.YarnUtils;

public class TezWordCount {
	private final static Log logger = LogFactory.getLog(TezWordCount.class);
	public static void main(String[] args) throws Exception {
		String file = "sample-256.txt";
		if (args.length > 0){
			file = args[0];
		}
		
		String appName = "WordCount";
		String outputPath = appName + "_out";
		DAG dag = new DAG(appName);
		TezConfiguration tezConfiguration = new TezConfiguration(new YarnConfiguration());
		FileSystem fs = FileSystem.get(tezConfiguration);
		Path inputPath = fs.makeQualified(new Path(file));
		logger.info("Counting words in " + inputPath);
		logger.info("Building local resources");
		Map<String, LocalResource> localResources = YarnUtils.createLocalResources(fs, "stark-cp");
		logger.info("Done building local resources");
		TezClient tezClient = new TezClient(appName, tezConfiguration);
		tezClient.addAppMasterLocalResources(localResources);
		tezClient.start();
		
		String user = null;
		try {
			user = UserGroupInformation.getCurrentUser().getShortUserName();
		} catch (Exception e) {
			throw new IllegalStateException("Failed to get current user", e);
		}
		String stagingDirStr = Path.SEPARATOR + "user" + Path.SEPARATOR + user + Path.SEPARATOR+ ".staging" + Path.SEPARATOR
		        + Path.SEPARATOR + Long.toString(System.currentTimeMillis());
		tezConfiguration.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
		Path stagingDir = fs.makeQualified(new Path(stagingDirStr));
		
		logger.info("Generating DAG");
		OrderedPartitionedKVEdgeConfigurer edgeConf = OrderedPartitionedKVEdgeConfigurer
		        .newBuilder(Text.class.getName(), IntWritable.class.getName(),
		            HashPartitioner.class.getName(), null).build();
		
		// MAPPER
		Configuration inputConf = new Configuration(tezConfiguration);
	    inputConf.set(FileInputFormat.INPUT_DIR, inputPath.toString());
	    InputDescriptor id = new InputDescriptor(MRInput.class.getName())
	        .setUserPayload(MRInput.createUserPayload(inputConf,
	            TextInputFormat.class.getName(), true, true));
	    InputInitializerDescriptor iid = new InputInitializerDescriptor(
	        MRInputAMSplitGenerator.class.getName());
	    Vertex mapper = new Vertex("tokenizer", new ProcessorDescriptor(
	            MyMapper.class.getName()), -1, MRHelpers.getMapResource(tezConfiguration));
	    mapper.addDataSource("MRInput", id, iid);
		mapper.setTaskLocalFiles(localResources);
		mapper.setTaskLaunchCmdOpts(MRHelpers.getMapJavaOpts(tezConfiguration));
		dag.addVertex(mapper);
		
		// REDUCER
		Configuration outputConf = new Configuration(tezConfiguration);
	    outputConf.set(FileOutputFormat.OUTDIR, outputPath);
	    OutputDescriptor od = new OutputDescriptor(MROutput.class.getName())
	      .setUserPayload(MROutput.createUserPayload(
	          outputConf, TextOutputFormat.class.getName(), true));
	    OutputCommitterDescriptor ocd = new OutputCommitterDescriptor(MROutputCommitter.class.getName());
	    Vertex reducer = new Vertex("summer",
	            new ProcessorDescriptor(
	                MyReducer.class.getName()), 2, MRHelpers.getReduceResource(tezConfiguration));
	    reducer.addDataSink("MROutput", od, ocd);
		reducer.setTaskLocalFiles(localResources);
		reducer.setTaskLaunchCmdOpts(MRHelpers.getMapJavaOpts(tezConfiguration));
	    dag.addVertex(reducer);
	    
	    Edge edge = new Edge(mapper, reducer, edgeConf.createDefaultEdgeProperty());
    	dag.addEdge(edge);
    	logger.info("Done generating DAG");
    	
    	logger.info("Waitin for Tez session");
    	tezClient.waitTillReady();
    	logger.info("Submitting DAG");
    	DAGClient dagClient = tezClient.submitDAG(dag);
    	dagClient.waitForCompletion();
    	logger.info("Finished DAG");
    	
    	DAGStatus dagStatus = dagClient.waitForCompletionWithAllStatusUpdates(null);
        if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
        	logger.info("DAG diagnostics: " + dagStatus.getDiagnostics());
        }
    	
    	RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path(outputPath), false);
    	while (iter.hasNext()){
    		LocatedFileStatus status = iter.next();
    		if (status.isFile()){
    			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
    			String line;
    			int counter = 0;
    			logger.info("Sampled results: ");
    			while ((line = reader.readLine()) != null && counter++ < 20){
    				logger.info(line);		
    			}
    			logger.info(". . . . . .");
    		}
    	}
    	
    	fs.deleteOnExit(stagingDir);
    	tezClient.stop();
	}
	
	public static class MyMapper extends SimpleMRProcessor {
	    IntWritable one = new IntWritable(1);
	    Text word = new Text();

	    @Override
	    public void run() throws Exception {
	      Preconditions.checkArgument(getInputs().size() == 1);
	      Preconditions.checkArgument(getOutputs().size() == 1);
	      MRInput input = (MRInput) getInputs().values().iterator().next();
	      KeyValueReader kvReader = input.getReader();
	      Output output = getOutputs().values().iterator().next();
	      KeyValueWriter kvWriter = (KeyValueWriter) output.getWriter();
	      while (kvReader.next()) {
	        StringTokenizer itr = new StringTokenizer(kvReader.getCurrentValue().toString());
	        while (itr.hasMoreTokens()) {
	          word.set(itr.nextToken());
	          kvWriter.write(word, one);
	        }
	      }
	    }
	  }

	  public static class MyReducer extends SimpleMRProcessor {
	    @Override
	    public void run() throws Exception {
	      Preconditions.checkArgument(getInputs().size() == 1);
	      MROutput out = (MROutput) getOutputs().values().iterator().next();
	      KeyValueWriter kvWriter = out.getWriter();
	      KeyValuesReader kvReader = (KeyValuesReader) getInputs().values().iterator().next().getReader();
	      while (kvReader.next()) {
	        Text word = (Text) kvReader.getCurrentKey();
	        int sum = 0;
	        for (Object value : kvReader.getCurrentValues()) {
	          sum += ((IntWritable) value).get();
	        }
	        kvWriter.write(word, new IntWritable(sum));
	      }
	    }
	  }

}
