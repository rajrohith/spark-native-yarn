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

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

import org.apache.spark.tez.io.TypeAwareStreams.TypeAwareObjectInputStream;
import org.apache.tez.runtime.api.ProcessorContext;

import scala.Tuple2;

/**
 * 
 */
public class TezUtils {

	/**
	 * 
	 * @param context
	 * @return
	 */
	public static byte[] getPayloadBytes(ProcessorContext context) {
		ByteBuffer payload = context.getUserPayload().getPayload();
		payload.rewind();
		byte[] taskBytes = new byte[payload.capacity()];
		payload.get(taskBytes);
		return taskBytes;
	}
	
	/**
	 * 
	 * @param context
	 * @return
	 * @throws Exception
	 */
	public static Tuple2<Object, Object> deserializePayload(ProcessorContext context) throws Exception {

		ByteBuffer payloadBuffer = ByteBuffer.wrap(getPayloadBytes(context));
		byte segmentCount = payloadBuffer.get();
		int taskLength = payloadBuffer.getInt();
		byte[] objectBytes = new byte[taskLength];
		payloadBuffer.get(objectBytes);
		ByteArrayInputStream bis = new ByteArrayInputStream(objectBytes);
		TypeAwareObjectInputStream is = new TypeAwareObjectInputStream(bis);
	    Object task = is.readObject();
	    Object partitioner = null;
	    is.close();
	    if (segmentCount == 2){
	    	objectBytes = new byte[payloadBuffer.getInt()];
	    	payloadBuffer.get(objectBytes);	
	    	bis = new ByteArrayInputStream(objectBytes);
	    	is = new TypeAwareObjectInputStream(bis);
	    	partitioner = is.readObject();
	    	is.close();
	    }
	    
		return new Tuple2<Object, Object>(task, partitioner);
	}
}
