package com.hortonworks.tez.spark.processor;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;

import com.google.common.base.Preconditions;
import com.hortonworks.spark.tez.FunctionHelper;

/**
 * Implementation of {@link SimpleMRProcessor} which delegates 
 * 
 * @author Oleg Zhurakousky
 *
 */
public class FunctionDelegatingMapProcessor extends SimpleMRProcessor {
	
	private volatile FunctionHelper functionHelper;
	
	private volatile Configuration configuration;
	
	
	public void initialize(TezProcessorContext processorContext) throws java.lang.Exception{
		super.initialize(processorContext);
		this.functionHelper = new FunctionHelper(this.getContext().getTaskVertexIndex());
//		this.configuration = TezUtils.createConfFromUserPayload(this.getContext().getUserPayload()); 
		
	}

	@Override
	public void run() throws Exception {
//		try {
//			Object o = this.configuration.get("tez.runtime.intermediate-output.key.class");
//			System.out.println(o);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		System.out.println("USER Payload: " + this.getContext().getUserPayload());
		System.out.println("UNIQUE ID: " + this.getContext().getUniqueIdentifier());
		System.out.println("UNIQUE Vertex ID: " + this.getContext().getTaskVertexIndex());
		System.out.println("**************** STARTING TokenProcessor: " + new Date());
		Preconditions.checkArgument(getInputs().size() == 1);
		Preconditions.checkArgument(getOutputs().size() == 1);
		MRInput input = (MRInput) getInputs().values().iterator().next();
		KeyValueReader kvReader = input.getReader();
		OnFileSortedOutput output = (OnFileSortedOutput) getOutputs().values()
				.iterator().next();
		KeyValueWriter kvWriter = (KeyValueWriter) output.getWriter();
		while (kvReader.next()) {
			String part = kvReader.getCurrentValue().toString();	
			Iterable<?> results = this.functionHelper.applyFunction1(part, kvWriter);
			
			for (Object result : results) {
//				Tuple2 t = null;
				System.out.println(result);
			}
			
			
			try {
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("**************** EXITING TokenProcessor: " + new Date());
	}
}
