package org.apache.spark.tez.test.utils;

import java.io.File;

import org.apache.spark.rdd.RDD;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class MockRddUtils {

	public static RDD<?> stubSaveAsTexFile(RDD<?> rdd, final String path) {
		RDD<?> sRdd = Mockito.spy(rdd);
		Mockito.doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				File file = new File(path);
				file.createNewFile();
				file.deleteOnExit();
				return null;
			}
		}).when(sRdd).saveAsTextFile(Mockito.anyString());
		return sRdd;
	}
}
