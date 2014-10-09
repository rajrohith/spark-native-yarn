package org.apache.spark.tez.test.utils;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;

import org.apache.spark.tez.io.TypeAwareWritable;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValuesReader;

public class TestLogicalInputWithKVsReader implements LogicalInput{
	
	@Override
	public void start() throws Exception {
		//noop
	}

	@Override
	public Reader getReader() throws Exception {
		
		return new KeyValuesReader() {
			
			private boolean hasNext = true;
			
			@Override
			public boolean next() throws IOException {
				if (hasNext){
					hasNext = false;
					return true;
				}
				else {
					return false;
				}
			}
			
			@Override
			public Iterable<Object> getCurrentValues() throws IOException {
				
				TypeAwareWritable<Object> foo = createWritable(false);
				foo.setValue("foo");
				TypeAwareWritable<Object> bar = createWritable(false);
				bar.setValue("bar");
				Object[] values = new Object[]{foo,bar};
				return Arrays.asList(values);
			}
			
			@Override
			public Object getCurrentKey() throws IOException {
				TypeAwareWritable<Object> keyWritable = createWritable(true);
				keyWritable.setValue(1);
				return keyWritable;
			}
		};
	}
	
	@SuppressWarnings("unchecked")
	private TypeAwareWritable<Object> createWritable(boolean key) {
		String className = key ? "org.apache.spark.tez.io.KeyWritable" 
				: "org.apache.spark.tez.io.ValueWritable";
		try {
			Class<?> clazz = Class.forName(className);
			Constructor<?> ctr = clazz.getDeclaredConstructor();
			ctr.setAccessible(true);
			TypeAwareWritable<Object> taw = (TypeAwareWritable<Object>) ctr.newInstance();
			return taw;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
}
