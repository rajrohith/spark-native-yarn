package com.hortonworks.spark.tez.processor;

import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkUtils;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.Writer;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.Assert;

import com.hortonworks.spark.tez.TezThreadLocalContext;

public class TezSparkProcessor extends SimpleMRProcessor {
	@Override
	public void run() throws Exception {
		try {
			this.doRun();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException("Failed to execute processor for Vertex " + this.context.getTaskVertexIndex(), e);
		}
	}
	
	private void doRun() throws Exception {
		System.out.println("######## RUNNING PROCESSOR ########");
		
		int vertextId = this.context.getTaskVertexIndex();
		String serializedTaskName = "SparkTask_" + vertextId + ".ser";
		ClassPathResource serializedTask = new ClassPathResource(serializedTaskName, Thread.currentThread().getContextClassLoader());
		Assert.isTrue(serializedTask.exists(), "Can't locate serialized task '" + serializedTaskName + "' on the classpath");
		SparkUtils.createSparkEnv();
		Object vertexTask = SparkUtils.deserializeSparkTask(ByteBuffer.wrap(IOUtils.toByteArray(serializedTask.getInputStream())));
		
		LogicalOutput output = this.getOutputs().values().iterator().next();	
		Writer writer = output.getWriter();	
		
		TezThreadLocalContext.setWriter(writer);	
		TezThreadLocalContext.setInputs(this.getInputs());
		
		SparkUtils.runTask(vertexTask);
		
		TezThreadLocalContext.removeInputs();
		TezThreadLocalContext.removeWriter();
	}
}