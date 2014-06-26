package com.hortonworks.tez;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.committer.MROutputCommitter;
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.library.input.ShuffledMergedInput;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.hortonworks.tez.spark.SumProcessor;
import com.hortonworks.tez.template.InputProcessor;
import com.hortonworks.tez.utils.YarnUtils;

public class TezDagBuilder {
	
	private final Log logger = LogFactory.getLog(TezDagBuilder.class);

	private final String inputPath;
	
	private final String outputPath;
	
	private final TezContext tezContext;
	
	private Map<String, LocalResource> localResources;
	
	private DAG dag;

	
	private String[] classpathExclusions;
	
	public TezDagBuilder(TezContext tezContext, String inputPath, String outputPath) {
//		Assert.hasText(tezContext.get, "'applicationName' must not be null or empty");
		Assert.hasText(inputPath, "'inputPath' must not be null or empty");
		this.tezContext = tezContext;
		
		this.inputPath = inputPath;
		if (!StringUtils.hasText(outputPath)){
			this.outputPath = this.tezContext.getApplicationName() + "/out";
		}
		else {
			this.outputPath = outputPath;
		}
		
		this.tezContext.getTezConfiguration().set(FileInputFormat.INPUT_DIR, this.inputPath);
		this.tezContext.getTezConfiguration().set(FileOutputFormat.OUTDIR, this.outputPath);
		
		this.initClasspathExclusions();
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
	private void provisionAndLocalizeUserFunctions(){
		File rootDir = new File(System.getProperty("user.dir"));
		String[] files = rootDir.list();
		for (String fileName : files) {
			if (fileName.endsWith(".ser")){
				File serFunction = new File(rootDir, fileName);
				this.provisionAndAddToLocalResources(serFunction);
				serFunction.delete();
			}
		}
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
		Path provisionedPath = YarnUtils.provisionResource(file, this.tezContext.getFileSystem(), 
				this.tezContext.getApplicationName(), this.tezContext.getApplicationId());
		LocalResource localResource = YarnUtils.createLocalResource(this.tezContext.getFileSystem(), provisionedPath);
		this.localResources.put(provisionedPath.getName(), localResource);
	}
	
	/**
	 * 
	 */
	public void build(){
		try {		
			Path[] provisionedResourcesPaths = YarnUtils.
					provisionClassPath(this.tezContext.getFileSystem(), 
							this.tezContext.getApplicationName(), this.tezContext.getApplicationId(), this.classpathExclusions);
			this.localResources = 
					YarnUtils.createLocalResources(tezContext.getFileSystem(), provisionedResourcesPaths);
			
			this.provisionAndLocalizeScalaLib();
			this.provisionAndLocalizeUserFunctions();
			
						
			InputDescriptor id = new InputDescriptor(MRInput.class.getName())
					.setUserPayload(MRInput.createUserPayload(this.tezContext.getTezConfiguration(),
							TextInputFormat.class.getName(), true, true));
			
			OutputDescriptor od = new OutputDescriptor(MROutput.class.getName())
					.setUserPayload(MROutput.createUserPayload(this.tezContext.getTezConfiguration(),
							TextOutputFormat.class.getName(), true));
			
			byte[] intermediateDataPayload = MRHelpers.
					createMRIntermediateDataPayload(this.tezContext.getTezConfiguration(), Text.class.getName(), IntWritable.class.getName(), true, null, null);
			
			Vertex tokenizerVertex = new Vertex("tokenizer",
					new ProcessorDescriptor(InputProcessor.class.getName()), -1,
					MRHelpers.getMapResource(this.tezContext.getTezConfiguration()));
			tokenizerVertex.setJavaOpts(MRHelpers.getMapJavaOpts(this.tezContext.getTezConfiguration()));
			tokenizerVertex.addInput("MRInput", id, MRInputAMSplitGenerator.class);
			tokenizerVertex.setTaskLocalResources(localResources);
			
			
			Vertex summerVertex = 
					new Vertex("summer", new ProcessorDescriptor(SumProcessor.class.getName()), 1, MRHelpers.getReduceResource(this.tezContext.getTezConfiguration()));
			summerVertex.setJavaOpts(MRHelpers.getReduceJavaOpts(this.tezContext.getTezConfiguration()));
			summerVertex.addOutput("MROutput", od, MROutputCommitter.class);
			summerVertex.setTaskLocalResources(localResources);
			
			DAG dag = new DAG("WordCount");
			dag.addVertex(tokenizerVertex)
					.addVertex(summerVertex)
					.addEdge(
							new Edge(
									tokenizerVertex,
									summerVertex,
									new EdgeProperty(
											DataMovementType.SCATTER_GATHER,
											DataSourceType.PERSISTED,
											SchedulingType.SEQUENTIAL,
											new OutputDescriptor(OnFileSortedOutput.class.getName())
													.setUserPayload(intermediateDataPayload),
											new InputDescriptor(ShuffledMergedInput.class.getName())
													.setUserPayload(intermediateDataPayload))));
			this.dag = dag;
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to buidl Tez DAG", e);
		}
	}
	
	public void run(){
		AMConfiguration amConfig = new AMConfiguration(null, localResources, tezContext.getTezConfiguration(), tezContext.getCredentials());

		TezSessionConfiguration sessionConfig = new TezSessionConfiguration(amConfig, tezContext.getTezConfiguration());
		TezSession tezSession = new TezSession("WordCountSession", tezContext.getApplicationId(), sessionConfig);
		try {
			tezSession.start();
		} catch (Exception e) {
			throw new IllegalStateException("Failed to start Tez session", e);
		}
		
		DAGClient dagClient = null;
		
		try {
			if (tezContext.getFileSystem().exists(new Path(outputPath))) {
				throw new FileAlreadyExistsException("Output directory " + outputPath + " already exists");
			}

			System.out.println("Tez Session: " + tezSession.getSessionStatus());
			tezSession.waitTillReady();
			dagClient = tezSession.submitDAG(this.dag);

			// monitoring
			DAGStatus dagStatus = dagClient.waitForCompletionWithAllStatusUpdates(null);
			if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
				System.out.println("DAG diagnostics: " + dagStatus.getDiagnostics());
				return;
			} 
			else {
				BufferedReader bis = new BufferedReader(new InputStreamReader(tezContext.getFileSystem().open(new Path(tezContext.getApplicationName() + "/out/part-v001-o000-r-00000"))));
				String line;
				while ((line = bis.readLine()) != null){
					System.out.println("LINE: " + line);
				}
				bis.close();
			}
			
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to execute DAG", e);
		}
		finally {
			try {
				tezContext.close();
			} catch (Exception e) {
				// ignore
			}
			try {
				tezSession.stop();
			} catch (Exception e) {
				// ignore
			}
			
		}
	}
}
