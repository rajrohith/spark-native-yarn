package org.apache.spark.tez.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.io.output.ByteArrayOutputStream;

@SuppressWarnings("unchecked")
public abstract class TypeAwareWritable<T> implements NewWritable<T> {
	
    protected T value;
	
	private byte valueType;
	
	private Class<?> valueTypeClass;

	private final ValueEncoder valueEncoder = new ValueEncoder();

	public void setValue(T value) {
		this.value = value;
		this.determineValueType(value);
		if (this.valueType == 0){
			this.valueType = this.valueEncoder.valueType;
			if (this.valueType != NULL){
				this.valueTypeClass = value.getClass();
			}
		} else if (this.valueEncoder.valueType != this.valueType) {
			throw new IllegalArgumentException("This instance of KeyWritable can not support " 
					+ value.getClass() + " since it is already setup for " + this.valueTypeClass);
		}
	}
	
	public T getValue() {
		return this.value;
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
		else if (value == null){
			this.valueEncoder.valueType = NULL;
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
	
	/**
	 * 
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		if (this.valueEncoder.valueBytes != null){
			out.writeByte(this.valueType);
			out.write(this.valueEncoder.valueBytes);
		}
	}

	/**
	 * 
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		if (this.valueEncoder.valueType != NULL) {
			this.valueType = in.readByte();
			switch (this.valueType) {
			case INTEGER:
				this.value = (T) Integer.valueOf(in.readInt());
				break;
			case LONG:
				this.value = (T) Long.valueOf(in.readLong());
				break;
			case OBJECT:
				try {
					ObjectInputStream ois = new ObjectInputStream(
							(DataInputStream) in);
					T value = (T) ois.readObject();
					this.value = (T) value;
				} catch (Exception e) {
					throw new IllegalStateException(
							"Failed to deserialize value", e);
				}

				break;
			default:
				throw new IllegalStateException(
						"Unsupported or unrecognized value type: "
								+ this.valueType);
			}
		}
	}
	
	@Override
	public String toString(){
		if (this.value == null){
			return null;
		} else {
			return this.value.toString();
		}
	}
	
	private final static byte INTEGER = -128;
	private final static byte STRING = -127;
	private final static byte LONG = -126;
	private final static byte OBJECT = -125;
	private final static byte NULL = -124;
	
	/**
	 */
	private class ValueEncoder {
		private byte valueType;
		private byte[] valueBytes;
	}
	
}
