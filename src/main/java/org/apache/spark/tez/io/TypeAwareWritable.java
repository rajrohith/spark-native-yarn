package org.apache.spark.tez.io;

public interface TypeAwareWritable<T> {

	void setValue(T value);
	
	T getValue();
	
}
