package org.apache.spark.tez.test.utils;

import java.io.IOException;

import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.library.api.KeyValueWriter;

public class TestLogicalOutput implements LogicalOutput {

	@Override
	public void start() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Writer getWriter() throws Exception {
		return new KeyValueWriter() {
			
			@Override
			public void write(Object key, Object value) throws IOException {
				// TODO Auto-generated method stub
				
			}
		};
	}

}
