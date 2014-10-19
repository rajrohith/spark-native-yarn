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

import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partitioner;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.ProcessorContext;

/**
 * 
 */
public class TezHelper {
	
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
		Path taskPath = SparkUtils.serializeToFs(vertexTask, fs, new Path(appName + "/tasks/" + vertexDescriptor.getVertexNameIndex() + ".ser"));
		UserPayload payload = UserPayload.create(ByteBuffer.wrap(taskPath.toString().getBytes()));
		return payload;
	}

	/**
	 * 
	 * @param context
	 * @return
	 * @throws Exception
	 */
	public static TezTask<?> deserializeTask(ProcessorContext context, FileSystem fs) throws Exception {
		ByteBuffer payloadBuffer = context.getUserPayload().getPayload();
		byte[] payloadBytes = new byte[payloadBuffer.capacity()];
		payloadBuffer.get(payloadBytes);
		String taskPath = new String(payloadBytes);
		InputStream is = fs.open(new Path(taskPath));
		TezTask<?> task = (TezTask<?>) SparkUtils.deserialize(is);
		is.close();
		return task;
	}
}
