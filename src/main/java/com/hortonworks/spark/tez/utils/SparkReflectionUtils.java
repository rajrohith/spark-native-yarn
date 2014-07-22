/**
 * 
 */
package com.hortonworks.spark.tez.utils;

import java.lang.reflect.Field;
import java.util.StringTokenizer;

import org.springframework.util.ReflectionUtils;

/**
 * @author ozhurakousky
 *
 */
public class SparkReflectionUtils {

	/**
	 * 
	 * @param rootClass
	 * @param fieldPath
	 */
	public static Object getFieldValue(Object rootObject, String fieldPath){
		StringTokenizer tokenizer = new StringTokenizer(fieldPath, ".");
		Object fieldValue = null;
		Object objectToEvaluate = rootObject;
		try {
			while (tokenizer.hasMoreTokens()){
				String token = tokenizer.nextToken();
				Field field = ReflectionUtils.findField(objectToEvaluate.getClass(), token);
				field.setAccessible(true);
				objectToEvaluate = field.get(objectToEvaluate);
				fieldValue = objectToEvaluate;
			}
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
		return fieldValue;
	}
}
