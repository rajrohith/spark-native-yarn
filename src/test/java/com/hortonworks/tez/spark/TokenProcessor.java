package com.hortonworks.tez.spark;

import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;

import com.google.common.base.Preconditions;

public class TokenProcessor extends SimpleMRProcessor {
	IntWritable one = new IntWritable(1);
	Text word = new Text();

	@Override
	public void run() throws Exception {
		System.out.println("**************** STARTING TokenProcessor: " + new Date());
		Preconditions.checkArgument(getInputs().size() == 1);
		Preconditions.checkArgument(getOutputs().size() == 1);
		MRInput input = (MRInput) getInputs().values().iterator().next();
		KeyValueReader kvReader = input.getReader();
		OnFileSortedOutput output = (OnFileSortedOutput) getOutputs().values()
				.iterator().next();
		KeyValueWriter kvWriter = (KeyValueWriter) output.getWriter();
		while (kvReader.next()) {
			StringTokenizer itr = new StringTokenizer(kvReader
					.getCurrentValue().toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				kvWriter.write(word, one);
			}
		}
		System.out.println("**************** EXITING TokenProcessor: " + new Date());
	}

}
