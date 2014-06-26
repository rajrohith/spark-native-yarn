package com.hortonworks.tez.template;

import java.util.Date;

import org.apache.spark.FunctionHelper;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;

import com.google.common.base.Preconditions;

public class InputProcessor extends SimpleMRProcessor {
	
	private volatile FunctionHelper functionHelper;
	
	
	public void initialize(TezProcessorContext processorContext) throws java.lang.Exception{
		super.initialize(processorContext);
		this.functionHelper = new FunctionHelper(this.getContext().getTaskVertexIndex());
	}

	@Override
	public void run() throws Exception {
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
			try {
				this.functionHelper.applyFunction1(part, kvWriter);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("**************** EXITING TokenProcessor: " + new Date());
	}
}
