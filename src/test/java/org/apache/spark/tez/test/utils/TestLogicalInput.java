package org.apache.spark.tez.test.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValueReader;

public class TestLogicalInput implements LogicalInput{
	
	private final BufferedReader reader;
	
	public TestLogicalInput(URI uri) {
		try {
			reader = new BufferedReader(new FileReader(new File(uri)));
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public void start() throws Exception {
		//noop
	}

	@Override
	public Reader getReader() throws Exception {
		
		return new KeyValueReader() {
			String line = "";
			int counter;
			@Override
			public boolean next() throws IOException {
				counter += line.length();
				line = reader.readLine();	
				return line != null;
			}
			
			@Override
			public Object getCurrentValue() throws IOException {
				return line;
			}
			
			@Override
			public Object getCurrentKey() throws IOException {
				return counter;
			}
		};
	}

}
