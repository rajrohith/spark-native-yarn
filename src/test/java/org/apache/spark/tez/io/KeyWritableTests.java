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
package org.apache.spark.tez.io;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.TreeSet;

import org.junit.Test;

public class KeyWritableTests {
	
	@Test
	public void validateSortingWithPrimitives() throws Exception {
		int size = 10;
		Random random = new Random();
		TreeSet<KeyWritable> set = new TreeSet<KeyWritable>();
		for (int i = 0; i < size; i++) {
			KeyWritable k = new KeyWritable();
			k.setValue(random.nextInt(100000));
			set.add(k);
		}
		assertEquals(size, set.size());
		int previous = -1;
		for (KeyWritable keyWritable : set) {
			int currentValue = (Integer) keyWritable.getValue();
			assertTrue(currentValue > previous);
			previous = currentValue;
		}
	}
	
	@Test
	public void validateSortingWithObjects() throws Exception {
		String[] values = "STARK project represents an extension to Apache Spark which enables DAGs assembled using SPARK API to run on Apache Tez".split(" ");
		KeyWritable[] kwValues = new KeyWritable[values.length];
		for (int i = 0; i < values.length; i++) {
			KeyWritable k = new KeyWritable();
			k.setValue(values[i]);
			kwValues[i] = k;
		}
		Arrays.sort(kwValues);
		// spot check
		assertEquals("API", kwValues[0].getValue());
		// make sure duplicates are preserved
		assertEquals("Apache", kwValues[1].getValue());
		assertEquals(kwValues[1].getValue(), kwValues[2].getValue());
		assertEquals("extension", kwValues[11].getValue());
	}
	
	@Test
	public void validateReadWritesWithPrimitives() throws Exception {
		int size = 10;
		Random random = new Random();
		int[] intArray = new int[size];
		for (int i = 0; i < intArray.length; i++) {
			intArray[i] = random.nextInt(100000);
		}
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		for (int val : intArray) {
			KeyWritable k = new KeyWritable();
			k.setValue(val);
			k.write(dos);
		}
		byte[] values = bos.toByteArray();
		assertEquals(50, values.length);
		ByteArrayInputStream bis = new ByteArrayInputStream(values);
		DataInputStream dis = new DataInputStream(bis);
		for (int i = 0; i < size; i++) {
			KeyWritable k = new KeyWritable();
			k.readFields(dis);
			assertEquals(intArray[i], k.getValue());
		}
	}
	
	@Test
	public void validateReadWritesWithObject() throws Exception {
		String[] values = "STARK project represents an extension to Apache Spark which enables DAGs assembled using SPARK API to run on Apache Tez".split(" ");
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		for (String val : values) {
			KeyWritable k = new KeyWritable();
			k.setValue(val);
			k.write(dos);
		}
		byte[] writtenValues = bos.toByteArray();
		ByteArrayInputStream bis = new ByteArrayInputStream(writtenValues);
		DataInputStream dis = new DataInputStream(bis);
		for (int i = 0; i < values.length; i++) {
			KeyWritable k = new KeyWritable();
			k.readFields(dis);
			assertEquals(values[i], k.getValue());
		}
	}

}
