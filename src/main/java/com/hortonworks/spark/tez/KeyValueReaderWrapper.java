package com.hortonworks.spark.tez;

import java.util.Iterator;

import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValuesReader;



public class KeyValueReaderWrapper {
	private final KeyValuesReader kvsReader;
	
	private final KeyValueReader kvReader;
	
	private Iterator<Object> valuesIterator;
	
	public KeyValueReaderWrapper(Reader reader) {	
		if (reader instanceof KeyValueReader){
			this.kvsReader = null;
			this.kvReader = (KeyValueReader) reader;
		}
		else {
			this.kvReader = null;
			this.kvsReader = (KeyValuesReader) reader;
		}
		if (this.kvsReader != null){
			try {			
				if (this.kvsReader.next()){
					this.valuesIterator = this.kvsReader.getCurrentValues().iterator();
				}
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
		else {
			try {			
				if (this.kvReader.next()){
					this.valuesIterator = new SingleValueIterator(this.kvReader.getCurrentValue());
				}
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
	}
	
	public boolean isSingleValue(){
		return this.kvReader != null;
	}
	
	public boolean hasNext(){
		boolean hasNext = false;
		try {
			if (this.valuesIterator != null && this.valuesIterator.hasNext()){
				hasNext = true;
			}
			else {
				if (this.kvsReader != null){
					if (this.kvsReader.next()){
						this.valuesIterator = this.kvsReader.getCurrentValues().iterator();
						hasNext = this.valuesIterator.hasNext();
					}
				}
				else {
					if (this.kvReader.next()){
						this.valuesIterator = new SingleValueIterator(this.kvReader.getCurrentValue());
						hasNext = this.valuesIterator.hasNext();
					}
				}
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return hasNext;
	}
	
	public Object nextValue(){
		return this.valuesIterator.next();
	}
	
	public Object nextKey(){
		try {
			Object key = null;
			if (this.kvReader != null){
				key = this.kvReader.getCurrentKey();
			}
			else {
				key = this.kvsReader.getCurrentKey();
			}
			return key;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
	
	private static class SingleValueIterator implements Iterator<Object> {
		private final Object value;
		private boolean hasNext = true;
		public SingleValueIterator(Object value) {
			this.value = value;
		}
		@Override
		public boolean hasNext() {
			return this.hasNext;
		}

		@Override
		public Object next() {
			this.hasNext = false;
			return this.value;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("remove is not supported");
		}
	}
}
