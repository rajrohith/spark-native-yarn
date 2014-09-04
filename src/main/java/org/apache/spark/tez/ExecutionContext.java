package org.apache.spark.tez;

import org.apache.tez.runtime.api.ObjectRegistry;

public class ExecutionContext {
	
	private static ObjectRegistry objectRegistry;
	
	public static ObjectRegistry getObjectRegistry() {
		return objectRegistry;
	}

	protected static void setObjectRegistry(ObjectRegistry objectRegistry) {
		ExecutionContext.objectRegistry = objectRegistry;
	}

	
}
