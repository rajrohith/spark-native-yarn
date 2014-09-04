package org.apache.spark.tez;

import java.lang.reflect.Method;

public class ClientExecutionContext {

	public static void setKey(Object function) {
		for (Method method : function.getClass().getDeclaredMethods()){
			System.out.println("HELLO: " + method);
		}
		
	}
}
