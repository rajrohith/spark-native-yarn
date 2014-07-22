package com.hortonworks.spark.tez;

import java.util.Iterator;

import org.apache.tez.runtime.library.api.KeyValuesReader;



public class KeyValueReaderWrapper {
	private final KeyValuesReader kvsReader;
	
	private Iterator<Object> valuesIterator;
	
	public KeyValueReaderWrapper(KeyValuesReader kvsReader) {
		this.kvsReader = kvsReader;
		try {
			if (this.kvsReader.next()){
				this.valuesIterator = this.kvsReader.getCurrentValues().iterator();
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
	
	public boolean hasNext(){
		boolean hasNext = false;
		try {
			if (this.valuesIterator.hasNext()){
				hasNext = true;
			}
			else {
				if (this.kvsReader.next()){
					this.valuesIterator = this.kvsReader.getCurrentValues().iterator();
					hasNext = this.valuesIterator.hasNext();
				}
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return hasNext;
	}
	
	public Object next(){
		return this.valuesIterator.next();
	}
}
