package com.hortonworks.spark.tez.utils;

import java.io.Serializable;

public class MutableInteger implements Serializable{
	 
	private int val;
 
	public MutableInteger(int val) {
		this.val = val;
	}
 
	public int get() {
		return val;
	}
 
	public void set(int val) {
		this.val = val;
	}
 
	//used to print value convinently
	public String toString(){
		return Integer.toString(val);
	}
}