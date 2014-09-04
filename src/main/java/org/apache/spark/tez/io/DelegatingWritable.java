package org.apache.spark.tez.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DelegatingWritable implements WritableComparable<DelegatingWritable> {
	
	private static Class<? extends WritableComparable> targetType;
	
	private volatile WritableComparable targetWritable;
	
	public static void setType(Class type) {
//		if (targetType != null){
//			throw new IllegalArgumentException("'targetType' has already been set for this instance of WritableComparable");
//		}
		if (type.isAssignableFrom(String.class)){
			targetType = Text.class;
		}
		else if (type.isAssignableFrom(Integer.class)){
			targetType = IntWritable.class;
		}
		else if (type.isAssignableFrom(NullWritable.class)){
			targetType = NullWritable.class;
		}
		else {
			throw new IllegalArgumentException("Unsupported type: " + type);
		}
	}
	
	public DelegatingWritable(){}
	
	public static boolean initialized() {
		return targetType != null;
//		return false;
	}
	
	public void setValue(Object value) {
		this.createInstanceIfNecessary();
		if (this.targetWritable instanceof Text){
			((Text)targetWritable).set((String) value);
		}
		else if (this.targetWritable instanceof IntWritable){
			((IntWritable)targetWritable).set((Integer)value);
		}
		else if (this.targetWritable instanceof NullWritable){
//			((IntWritable)targetWritable).set((Integer)value);
		}
		else {
			throw new IllegalArgumentException("Can't set value: + '" + value + "' to " + targetWritable);
		}
	}
	
	public Object getValue(){
		return targetWritable.toString();
	}

	public String toString(){
		this.createInstanceIfNecessary();
		return this.targetWritable.toString();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		this.createInstanceIfNecessary();
		this.targetWritable.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.createInstanceIfNecessary();
		this.targetWritable.readFields(in);
	}
	
	@Override
	public int compareTo(DelegatingWritable o) {
		this.createInstanceIfNecessary();
//		System.out.println("Comparing: " + this.getValue() + " - " + o.getValue());
		int res = this.targetWritable.compareTo(o.targetWritable);
		return res;
	}
	
	private void createInstanceIfNecessary() {
		if (targetWritable == null){
			try {
				if (targetType.isAssignableFrom(NullWritable.class)){
					targetWritable = NullWritable.get();
				}
				else {
					targetWritable = targetType.newInstance();
				}
			} catch (Exception e) {
				throw new IllegalStateException("Failed", e);
			}
		}
	}
}
