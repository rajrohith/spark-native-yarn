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
package org.apache.spark.tez;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partitioner;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.ProcessorContext;

/**
 * 
 */
public class TezUtils {
	
	/**
	 * 
	 * @param vertexDescriptor
	 * @param vertexName
	 * @param fs
	 * @return
	 */
	public static UserPayload serializeTask(VertexDescriptor vertexDescriptor, String vertexName, FileSystem fs, Partitioner partitioner, String appName){
		vertexDescriptor.setVertexNameIndex(vertexName);		
		TezTask<?> vertexTask = vertexDescriptor.getTask();
		vertexTask.setPartitioner(partitioner);
		ByteBuffer taskBuffer = SparkUtils.serialize(vertexTask);
		File serTask = ClassPathUtils.ser(taskBuffer, vertexDescriptor.getVertexNameIndex() + ".ser");
		Path taskPath = YarnUtils.provisionResource(serTask, fs, appName);
		UserPayload payload = UserPayload.create(ByteBuffer.wrap(taskPath.toString().getBytes()));
		return payload;
	}

	/**
	 * 
	 * @param context
	 * @return
	 * @throws Exception
	 */
	public static TezTask<?> deserializeTask(ProcessorContext context) throws Exception {
		ByteBuffer payloadBuffer = context.getUserPayload().getPayload();
		byte[] payloadBytes = new byte[payloadBuffer.capacity()];
		payloadBuffer.get(payloadBytes);
		String taskPath = new String(payloadBytes);
		URL taskUrl = new URL(taskPath);
		InputStream is = taskUrl.openStream();
		TezTask<?> task = (TezTask<?>) SparkUtils.deserialize(is);
		return task;
	}
}
