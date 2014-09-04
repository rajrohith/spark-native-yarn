package org.apache.spark.tez;

import java.nio.ByteBuffer;

import org.apache.tez.runtime.api.ProcessorContext;

public class TezUtils {

	public static byte[] getTaskBuffer(ProcessorContext context) {
		ByteBuffer payload = context.getUserPayload().getPayload();
		payload.rewind();
		byte[] taskBytes = new byte[payload.capacity()];
		payload.get(taskBytes);
		return taskBytes;
	}
}
