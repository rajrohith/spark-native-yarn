package org.apache.spark.tez.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.Writable;

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
class ValueWritable implements Writable {
	
	private Object value;
	
	private byte valueType;
	
	private Class<?> valueTypeClass;

	private final ValueEncoder valueEncoder = new ValueEncoder();
	
	/**
	 * 
	 * @param value
	 */
	public void setValue(Object value) {
		this.value = value;
		this.determineValueType(value);
		if (this.valueType == 0){
			this.valueType = this.valueEncoder.valueType;
			this.valueTypeClass = value.getClass();
		} else if (this.valueEncoder.valueType != this.valueType) {
			throw new IllegalArgumentException("This instance of ValueWritable can not support " 
					+ value.getClass() + " since it is already setup for " + this.valueTypeClass);
		}
	}
	
	/**
	 * 
	 * @return
	 */
	public Object getValue() {
		return this.value;
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
				Object value =  ois.readObject();
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
