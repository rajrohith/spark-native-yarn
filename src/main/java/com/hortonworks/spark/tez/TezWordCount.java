package com.hortonworks.spark.tez;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.HashMap;
//import java.util.HashMap;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.spark.SparkUtils;
import org.apache.spark.TezShuffleManager;
import org.apache.spark.tez.VertexTask;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Preconditions;
import com.hortonworks.spark.tez.utils.MutableInteger;
import com.hortonworks.spark.tez.utils.YarnUtils;

public class TezWordCount {
	private final static Log logger = LogFactory.getLog(TezWordCount.class);
	static String INPUT = "Input";
	static String OUTPUT = "Output";
	static String TOKENIZER = "Tokenizer";
	static String SUMMATION = "Summation";

	// private final static ExecutorService executor =
	// Executors.newCachedThreadPool();
	// private final static CountDownLatch latch = new CountDownLatch(2);
	public static void main(String[] args) throws Exception {
//		String file = args[0];
//		int red  = Integer.parseInt(args[1]);
//		String file = args[0];
		int red  = 50;
		
//		String file = "hdfs://cn105-10.l42scl.hortonworks.com:8020/user/zzhang/low1gb.txt";
		String file = "hdfs://cn105-10.l42scl.hortonworks.com:8020/user/zzhang/100gb.txt";
//		String file = "hdfs://cn105-10.l42scl.hortonworks.com:8020/user/zzhang/1tb.txt";
//		 String file = "foo.txt";
//		 String file = "sample-256.txt";
		

		String appName = "tez-wc";
		String outputPath = appName + "_out";
		final DAG dag = new DAG(appName);
		TezConfiguration tezConfiguration = new TezConfiguration(
				new YarnConfiguration());
		FileSystem fs = FileSystem.get(tezConfiguration);
		fs.delete(new Path(appName + "_out"), true);
//		fs.delete(new Path("stark-cp"), true);

//		Path testFile = new Path(file);
//		fs.copyFromLocalFile(false, true, new Path("/Users/ozhurakousky/dev/spark-on-tez/" + file), testFile);

		System.out.println("STARTING JOB");

		Path inputPath = fs.makeQualified(new Path(file));
		logger.info("Counting words in " + inputPath);
		logger.info("Building local resources");
		Map<String, LocalResource> localResources = YarnUtils.createLocalResources(fs, "stark-cp");
		logger.info("Done building local resources");
		
		UserGroupInformation.setConfiguration(tezConfiguration);
		final TezClient tezClient = TezClient.create("WordCount", tezConfiguration);
		tezClient.addAppMasterLocalResources(localResources);
		tezClient.start();
		
		logger.info("Generating DAG");
		
		OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
		        .newBuilder(Text.class.getName(), IntWritable.class.getName(),
		            HashPartitioner.class.getName(), null).build();

//		edgeConf.
		
		DataSourceDescriptor dataSource = MRInput.createConfigBuilder(new Configuration(tezConfiguration), TextInputFormat.class, file).build();
		
		DataSinkDescriptor dataSink = MROutput.createConfigBuilder(new Configuration(tezConfiguration),TextOutputFormat.class, outputPath).build();
		
		Vertex mapper = Vertex.create(TOKENIZER, ProcessorDescriptor.create(
			        TokenProcessor.class.getName())).addDataSource(INPUT, dataSource);
		mapper.setTaskLocalFiles(localResources);
		dag.addVertex(mapper);
		
		Vertex reducer = Vertex.create(SUMMATION,
		        ProcessorDescriptor.create(SumProcessor.class.getName()), red)
		        .addDataSink(OUTPUT, dataSink);
		reducer.setTaskLocalFiles(localResources);
		dag.addVertex(reducer);
		
		dag.addEdge(Edge.create(mapper, reducer, edgeConf.createDefaultEdgeProperty()));
		
		logger.info("Done generating DAG");

		logger.info("Waitin for Tez session");
		tezClient.waitTillReady();
		logger.info("Submitting DAG");
		
		long start = System.currentTimeMillis();
		DAGClient dagClient = tezClient.submitDAG(dag);
		DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
		long stop = System.currentTimeMillis();
		float elapsed = ((float)((stop-start)/(1000*60)));
		logger.info("Finished DAG in " + elapsed + " minutes");
	
		if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
			logger.info("DAG diagnostics: " + dagStatus.getDiagnostics());
		}
		
		tezClient.stop();
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path(outputPath), false);
		int counter = 0;
		while (iter.hasNext() && counter++ < 20) {
			LocatedFileStatus status = iter.next();
			if (status.isFile()) {
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(fs.open(status.getPath())));
				String line;
				logger.info("Sampled results from " + status.getPath() + ":");
				while ((line = reader.readLine()) != null && counter++ < 20) {
					logger.info(line);
				}
				logger.info(". . . . . .");
			}

		}
	}

	public static class TokenProcessor extends SimpleProcessor {
		//static Map<String, MutableInteger> groups = new HashMap<String, MutableInteger>();
		static Map<String, MutableInteger> groups = new HashMap<String, MutableInteger>(100000);
//		DB db;
	    IntWritable count = new IntWritable(1);
	    Text word = new Text();

	    public TokenProcessor(ProcessorContext context) {
	      super(context);
	    }

	    @Override
	    public void run() throws Exception {
//	    	String value = UUID.randomUUID().toString();
//	    	Combiner.setValue(value);
//	    	System.out.println("==> IN MAPPER: " + Combiner.getValue());
	      Preconditions.checkArgument(getInputs().size() == 1);
	      Preconditions.checkArgument(getOutputs().size() == 1);
	      // the recommended approach is to cast the reader/writer to a specific type instead
	      // of casting the input/output. This allows the actual input/output type to be replaced
	      // without affecting the semantic guarantees of the data type that are represented by
	      // the reader and writer.
	      // The inputs/outputs are referenced via the names assigned in the DAG.
	      
	      
//	      TezShuffleManager shufleManager = new TezShuffleManager(null, null);
//			SparkUtils.createSparkEnv(shufleManager);
//	      
//	      ByteBuffer payload = this.getContext().getUserPayload().getPayload();
//			payload.rewind();
//			byte[] pBytes = new byte[payload.capacity()];
//			payload.get(pBytes);
//			VertexTask vertexTask = SparkUtils.deserializeSparkTask(pBytes, this.getContext().getTaskIndex());
	      
	      
	      
//	      KeyValueReader kvReader = (KeyValueReader) getInputs().get(INPUT).getReader();
//	      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(SUMMATION).getWriter();
	      
	      KeyValueReader kvReader = (KeyValueReader) getInputs().values().iterator().next().getReader();
	      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();

	      while (kvReader.next()) {
	        StringTokenizer itr = new StringTokenizer(kvReader.getCurrentValue().toString());
	        while (itr.hasMoreTokens()) {
	          String token = itr.nextToken();
	          MutableInteger intValue = new MutableInteger(1);
	          MutableInteger oldValue = groups.put(token, intValue);
//	          word.set(itr.nextToken());
	          // Count 1 every time a word is observed. Word is the key a 1 is the value
//	          kvWriter.write(word, count);
	          
	          if (oldValue != null){
	        	  intValue.set(oldValue.get()+1);
	          }
	        }
	      }
	      for (Map.Entry<String, MutableInteger> entry : groups.entrySet()) {
	    	  word.set(entry.getKey());
	    	  count.set(entry.getValue().get());
	    	  kvWriter.write(word, count);
	      }
//	      db.close();
	      groups.clear();
	    }

	  }

	  /*
	   * Example code to write a processor that commits final output to a data sink
	   * The SumProcessor aggregates the sum of individual word counts generated by 
	   * the TokenProcessor.
	   * The SumProcessor is connected to a DataSink. In this case, its an Output that
	   * writes the data via an OutputFormat to a data sink (typically HDFS). Thats why
	   * it derives from SimpleMRProcessor that takes care of handling the necessary 
	   * output commit operations that makes the final output available for consumers.
	   */
	  public static class SumProcessor extends SimpleMRProcessor {
	    public SumProcessor(ProcessorContext context) {
	      super(context);
	    }

	    @Override
	    public void run() throws Exception {
//	    	System.out.println("==> IN REDUCER: " + Combiner.getValue());
	      Preconditions.checkArgument(getInputs().size() == 1);
	      Preconditions.checkArgument(getOutputs().size() == 1);
//	      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(OUTPUT).getWriter();
//	      KeyValuesReader kvReader = (KeyValuesReader) getInputs().get(TOKENIZER).getReader();
	      // The KeyValues reader provides all values for a given key. The aggregation of values per key
	      // is done by the LogicalInput. Since the key is the word and the values are its counts in 
	      // the different TokenProcessors, summing all values per key provides the sum for that word.
	      
//	      TezShuffleManager shufleManager = new TezShuffleManager(null, null);
//			SparkUtils.createSparkEnv(shufleManager);
//	      ByteBuffer payload = this.getContext().getUserPayload().getPayload();
//			payload.rewind();
//			byte[] pBytes = new byte[payload.capacity()];
//			payload.get(pBytes);
//			VertexTask vertexTask = SparkUtils.deserializeSparkTask(pBytes, this.getContext().getTaskIndex());
	      
	      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();
	      KeyValuesReader kvReader = (KeyValuesReader) getInputs().values().iterator().next().getReader();
	      
	      while (kvReader.next()) {
	        Text word = (Text) kvReader.getCurrentKey();
	        int sum = 0;
	        for (Object value : kvReader.getCurrentValues()) {
	          sum += ((IntWritable) value).get();
	        }
	        kvWriter.write(word, new LongWritable(sum));
	      }
	      // deriving from SimpleMRProcessor takes care of committing the output
	      // It automatically invokes the commit logic for the OutputFormat if necessary.
	    }
	  }

	  static class Combiner {
		  private static volatile String v;
		  public static void setValue(String value) {
			  v = value;
		  }
		  public static String getValue(){
			  return v;
		  }
	  }
}
