package org.apache.spark.tez.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import scala.Tuple3;

public class ComparableObjectWritable<T> implements WritableComparable<ComparableObjectWritable<T>> {

	private static Tuple3<WritableComparable, Method, Method> members;
	
	@SuppressWarnings("rawtypes")
	public ComparableObjectWritable(){
		if (members == null){
			BytesWritable bw = new BytesWritable();
			@SuppressWarnings("unchecked")
			Tuple3<WritableComparable, Method, Method> t = 
					new Tuple3<WritableComparable, Method, Method>((WritableComparable<T>) bw, (Method)null, (Method)null);
			members = t;
		}
	}
	
	@SuppressWarnings("unchecked")
	public ComparableObjectWritable(Class<?> writableCandidate){
		try {
			WritableComparable wc;
			Method sm;
			Method gm;
			if (writableCandidate.isAssignableFrom(String.class)){
				System.out.println("CREATING TEXT");
				wc = (WritableComparable<T>) new Text();
				sm = wc.getClass().getDeclaredMethod("set", String.class);
				gm = wc.getClass().getDeclaredMethod("toString");
				Tuple3<WritableComparable, Method, Method> t = 
						new Tuple3<WritableComparable, Method, Method>(wc, sm, gm);
				members = t;
			}
			else if (writableCandidate.isAssignableFrom(Integer.class)){
				System.out.println("CREATING INT WRITABLE");
				wc = (WritableComparable<T>) new IntWritable();
				sm = wc.getClass().getDeclaredMethod("set", int.class);
				gm = wc.getClass().getDeclaredMethod("get");
				Tuple3<WritableComparable, Method, Method> t = 
						new Tuple3<WritableComparable, Method, Method>(wc, sm, gm);
				members = t;
			}
			else {
				throw new IllegalArgumentException("Type " + writableCandidate + " is not supported");
			}
		} catch (Exception e) {
			throw new IllegalStateException("Failed to create instance for type " + writableCandidate, e);
		}
	}
	
	@Override
	public int hashCode(){
		int h = this.getValue().hashCode();
//		System.out.println("hashcode: " + h);
		return h;
	}
	
	@Override
	public boolean equals(Object o) {
//		System.out.println("equals");
		return o instanceof ComparableObjectWritable && this.getValue().equals(((ComparableObjectWritable)o).getValue());
	}
	
	@SuppressWarnings("static-access")
	public void setValue(Object v) {
		try {
//			Text t = new Text(v.toString());
			members._2().invoke(this.members._1(), v);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid value " + v, e);
		}
	}

	public Object getValue() {
		try {
			return (Object) members._3().invoke(members._1());
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to get value", e);
		}
	}
	
	public String toString() {
		return this.getValue().toString();
	}
	
	@SuppressWarnings("static-access")
	@Override
	public void write(DataOutput out) throws IOException{
		members._1().write(out);
	}
	
	@SuppressWarnings("static-access")
	@Override
	public void readFields(DataInput in) throws IOException{
		members._1().readFields(in);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int compareTo(ComparableObjectWritable<T> o) {
		Comparable source = (Comparable) this.getValue();
		Comparable target = (Comparable) o.getValue();
		
		int cv = source.compareTo(target);
		System.out.println("compare: " + cv);
		return cv;
	}
}
