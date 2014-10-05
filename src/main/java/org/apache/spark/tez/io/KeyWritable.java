/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.tez.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.WritableComparable;

/**
 * This class represents a universal Writable with the goal of recognizing 
 * and properly writing/reading multiple types of data.
 * More documentation to follow as its design progresses, but at the moment the mechanism is simple:
 * 1. Prepend each value with a byte representing its type
 * 2. Write value data as byte[]
 * For fixed formats such as Int, Long etc., its standard byte representation is used (e.g., Int = 4 bytes)
 * For Objects (variable length) object serialization is used with custom se/deser to be exposed for 
 * customization and optimization
 * 
 * This class is not public nor it is meant/designed as thread-safe.
 */
@SuppressWarnings("rawtypes") 
class KeyWritable implements WritableComparable<KeyWritable> {
	
	private Comparable value;
	
	private byte valueType;
	
	private Class<?> valueTypeClass;

	private final ValueEncoder valueEncoder = new ValueEncoder();
	
	/**
	 * 
	 * @param value
	 */
	public void setValue(Comparable value) {
		this.value = value;
		this.determineValueType(value);
		if (this.valueType == 0){
			this.valueType = this.valueEncoder.valueType;
			this.valueTypeClass = value.getClass();
		} else if (this.valueEncoder.valueType != this.valueType) {
			throw new IllegalArgumentException("This instance of KeyWritable can not support " 
					+ value.getClass() + " since it is already setup for " + this.valueTypeClass);
		}
	}
	
	/**
	 * 
	 * @return
	 */
	public Comparable<?> getValue() {
		return this.value;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public int compareTo(KeyWritable o) {
		return this.value.compareTo(o.value);
	}

	/**
	 * 
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(this.valueType);
		out.write(this.valueEncoder.valueBytes);
	}

	/**
	 * 
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.valueType = in.readByte();
		switch (this.valueType) {
		case INTEGER:
			this.value = in.readInt();
			break;
		case LONG:
			this.value = in.readLong();
			break;
		case OBJECT:
			try {
				ObjectInputStream ois = new ObjectInputStream((DataInputStream)in);
				Comparable<?> value =  (Comparable<?>) ois.readObject();
				this.value = value;
			} catch (Exception e) {
				throw new IllegalStateException("Failed to deserialize value", e);
			}
			
			break;
		default:
			throw new IllegalStateException("Unsupported or unrecognized value type: " + this.valueType);
		}
	}
	
	public String toString(){
		return this.value.toString();
	}
	
	/**
	 * NOTE: The below code is temporary and both conversion and ser/deser will be exposed through externally 
	 * configurable framework!
	 * 
	 * @param value
	 * @return
	 */
	private void determineValueType(Object value){
		if (value instanceof Integer){
			this.valueEncoder.valueType = INTEGER;
			this.valueEncoder.valueBytes = ByteBuffer.allocate(4).putInt((Integer)value).array();
		} 
		else if (value instanceof Long) {
			this.valueEncoder.valueType = LONG;
			this.valueEncoder.valueBytes = ByteBuffer.allocate(8).putLong((Long)value).array();
		} 
		
		else {
			ObjectOutputStream oos = null;
			try {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				oos = new ObjectOutputStream(bos);
				oos.writeObject(value);
				this.valueEncoder.valueType = OBJECT;
				this.valueEncoder.valueBytes = bos.toByteArray();
				
			} catch (Exception e) {
				throw new IllegalStateException("Failed to serialize value: " + value, e);
			} finally {
				try {
					if (oos != null){
						oos.close();
					}
				} catch (Exception e2) {/*ignore*/}
			}
		}
	}
	
	private final static byte INTEGER = -128;
	private final static byte STRING = -127;
	private final static byte LONG = -126;
	private final static byte OBJECT = -125;

	/**
	 */
	private class ValueEncoder {
		private byte valueType;
		private byte[] valueBytes;
	}
}
