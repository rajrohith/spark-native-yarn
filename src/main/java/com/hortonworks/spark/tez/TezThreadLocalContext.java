package com.hortonworks.spark.tez;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.Writer;

public class TezThreadLocalContext {
	
	public static final ThreadLocal<Map<String, Object>> tl = new ThreadLocal<Map<String, Object>>();
	static {
		tl.set(new HashMap<String, Object>());
	}
	
	public static void setInputs(Map<String, LogicalInput> inputs) {
		TreeMap<Integer, LogicalInput> inputMap = new TreeMap<Integer, LogicalInput>();
		for (String inputIndexName : inputs.keySet()) {
			try {
				inputMap.put(Integer.parseInt(inputIndexName), inputs.get(inputIndexName));
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Vertex name must be parsable to Integer. Was: '" + inputIndexName + "'", e);
			}
		}
        tl.get().put("reader", inputMap);
    }

    public static void removeInputs() {
    	tl.get().remove("reader");
    }

    @SuppressWarnings("unchecked")
	public static Reader getReader() {
    	try {
    		Map<Integer, LogicalInput> inputs = (Map<Integer, LogicalInput>) tl.get().get("reader");
        	int inputIndex = inputs.keySet().iterator().next();
        	LogicalInput input = inputs.remove(inputIndex);
        	return input.getReader();
		} catch (Exception e) {
			throw new IllegalStateException("Failed to obtain reader", e);
		}
    }
    
    public static void setWriter(Writer writer) {
        tl.get().put("writer", writer);
    }

    public static void removeWriter() {
    	tl.get().remove("writer");
    }

    public static Writer getWriter() {
        return (Writer) tl.get().get("writer");
    }
}
