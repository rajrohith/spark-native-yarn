package com.hortonworks.tez.spark;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;

import com.google.common.base.Preconditions;

public class SumProcessor extends SimpleMRProcessor {
	@Override
	public void run() throws Exception {
		System.out.println("&&&&&&&&&&&&& IN SUM PROCESSOR");
		Preconditions.checkArgument(getInputs().size() == 1);
		MROutput out = (MROutput) getOutputs().values().iterator().next();
		KeyValueWriter kvWriter = out.getWriter();
		KeyValuesReader kvReader = (KeyValuesReader) getInputs().values()
				.iterator().next().getReader();
		while (kvReader.next()) {
			Text word = (Text) kvReader.getCurrentKey();
			int sum = 0;
			for (Object value : kvReader.getCurrentValues()) {
				sum += ((IntWritable) value).get();
			}
			kvWriter.write(word, new IntWritable(sum));
		}
		System.out.println("&&&&&&&&& EXITIN SUM");
	}
}