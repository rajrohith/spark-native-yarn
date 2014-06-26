/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.tez.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
import org.apache.tez.dag.api.TezConfiguration;
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

import com.hortonworks.tez.TezContext;
import com.hortonworks.tez.utils.YarnUtils;

public class TezWordCount {

	private DAG createDAG(TezConfiguration tezConf,
			Map<String, LocalResource> localResources, Path stagingDir,
			String inputPath, String outputPath) throws IOException {

		// INPUT
		Configuration inputConf = new Configuration(tezConf);
		inputConf.set(FileInputFormat.INPUT_DIR, inputPath);
		InputDescriptor id = new InputDescriptor(MRInput.class.getName())
				.setUserPayload(MRInput.createUserPayload(inputConf,
						TextInputFormat.class.getName(), true, true));

		// OUTPUT
		Configuration outputConf = new Configuration(tezConf);
		outputConf.set(FileOutputFormat.OUTDIR, outputPath);
		OutputDescriptor od = new OutputDescriptor(MROutput.class.getName())
				.setUserPayload(MROutput.createUserPayload(outputConf,
						TextOutputFormat.class.getName(), true));

		byte[] intermediateDataPayload = MRHelpers.createMRIntermediateDataPayload(tezConf, Text.class.getName(), IntWritable.class.getName(), true, null, null);
		
		Vertex tokenizerVertex = new Vertex("tokenizer",
				new ProcessorDescriptor(TokenProcessor.class.getName()), -1,
				MRHelpers.getMapResource(tezConf));
		tokenizerVertex.setJavaOpts(MRHelpers.getMapJavaOpts(tezConf));
		tokenizerVertex.addInput("MRInput", id, MRInputAMSplitGenerator.class);
		tokenizerVertex.setTaskLocalResources(localResources);

		Vertex summerVertex = new Vertex("summer", new ProcessorDescriptor(SumProcessor.class.getName()), 1, MRHelpers.getReduceResource(tezConf));
		summerVertex.setJavaOpts(MRHelpers.getReduceJavaOpts(tezConf));
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

		return dag;
	}
	/**
	 * 
	 * @param inputPath
	 * @param outputPath
	 * @return
	 * @throws Exception
	 */
	public boolean run(String inputPath, String outputPath) throws Exception {
		System.out.println("Running WordCount");
	
		TezContext tezContext = new TezContext("WordCount");
		System.out.println("Application ID: " + tezContext.getApplicationId());
//		TezClient tezClient = tezContext.getTezClient();

		Path[] provisionedResourcesPaths = YarnUtils.provisionClassPath(tezContext.getFileSystem(), tezContext.getApplicationName(), tezContext.getApplicationId());
		Map<String, LocalResource> localResources = YarnUtils.createLocalResources(tezContext.getFileSystem(), provisionedResourcesPaths);
		
		AMConfiguration amConfig = new AMConfiguration(null, localResources, tezContext.getTezConfiguration(), tezContext.getCredentials());

		TezSessionConfiguration sessionConfig = new TezSessionConfiguration(amConfig, tezContext.getTezConfiguration());
		TezSession tezSession = new TezSession("WordCountSession", tezContext.getApplicationId(), sessionConfig);
		tezSession.start();
		
		// GENERATE SAMPLE FILE
		Path testFile = new Path("tez_test.txt");
		FSDataOutputStream out = tezContext.getFileSystem().create(testFile, true);
		//10000000
		for (int i = 0; i < 10000; i++) {
			out.write("hello world ".getBytes());
		}
		out.close();
		//

		DAGClient dagClient = null;
		
		try {
			if (tezContext.getFileSystem().exists(new Path(outputPath))) {
				throw new FileAlreadyExistsException("Output directory " + outputPath + " already exists");
			}

			//localResources = new HashMap<String, LocalResource>();
			DAG dag = createDAG(tezContext.getTezConfiguration(), localResources, tezContext.getStagingDir(), inputPath, outputPath);
			
			System.out.println("Tez Session: " + tezSession.getSessionStatus());
			tezSession.waitTillReady();
			dagClient = tezSession.submitDAG(dag);
//			dagClient = tezClient.submitDAGApplication(dag, amConfig);

			// monitoring
			DAGStatus dagStatus = dagClient.waitForCompletionWithAllStatusUpdates(null);
			if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
				System.out.println("DAG diagnostics: " + dagStatus.getDiagnostics());
				return false;
			} 
			else {
				RemoteIterator<LocatedFileStatus> is = tezContext.getFileSystem().listFiles(new Path("tez_out2"), true);
				while (is.hasNext()) {
					System.out.println(is.next());
				}
			}
			BufferedReader bis = new BufferedReader(new InputStreamReader(tezContext.getFileSystem().open(new Path("tez_out2/part-v001-o000-r-00000"))));
			String line;
			while ((line = bis.readLine()) != null){
				System.out.println("LINE: " + line);
			}
			bis.close();
			return true;
		} 
		finally {
			tezContext.close();
			tezSession.stop();
		}
	}

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		TezWordCount job = new TezWordCount();
		job.run("tez_test.txt", "tez_out2");
	}
}
