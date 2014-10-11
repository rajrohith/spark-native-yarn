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

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.spark.tez.test.utils.Instrumentable;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Test;

/**
 *
 */
public class DAGBuilderTests extends Instrumentable {
	
	
	@Test
	public void foo() {
		System.out.println();
	}
	
//	@BeforeClass
//	public static void prepare(){
//		TezClientTestInstrumenter.instrumentForTests();
//	}

//	@SuppressWarnings("unchecked")
//	@Test(expected=IllegalStateException.class)
//	public void failWithTezClientNull() {
//		new DAGBuilder(null, mock(Map.class), mock(Configuration.class));
//	}
//	
//	@Test(expected=IllegalStateException.class)
//	public void failWithLocalResourcesMapNull() {
//		new DAGBuilder(mock(TezClient.class), null, mock(Configuration.class));
//	}
//	
//	@SuppressWarnings("unchecked")
//	@Test(expected=IllegalStateException.class)
//	public void failWithConfigurationNull() {
//		new DAGBuilder(mock(TezClient.class), mock(Map.class), null);
//	}
	
//	@Test
//	public void validateInitialization() {
//		TezConfiguration configuration = new TezConfiguration();
//		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
//		TezClient tezClient = this.mockTezClient("foo", configuration);
//		DAGBuilder dagBuilder = new DAGBuilder(tezClient, localResources, configuration);
//		
//		assertSame(tezClient, ReflectionUtils.getJavaFieldValue(dagBuilder, "tezClient"));
//		assertSame(configuration, ReflectionUtils.getJavaFieldValue(dagBuilder, "tezConfiguration"));
//		assertSame(localResources, ReflectionUtils.getJavaFieldValue(dagBuilder, "localResources"));
//		assertTrue(((String)ReflectionUtils.getJavaFieldValue(dagBuilder, "applicationInstanceName")).startsWith("foo_"));
//		assertNotNull(ReflectionUtils.getJavaFieldValue(dagBuilder, "dag"));
//	}
	
//	@Test
//	public void validateBuild() {
//		TezConfiguration configuration = new TezConfiguration();
//		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
//		TezClient tezClient = this.mockTezClient("foo");
//		DAGBuilder dagBuilder = new DAGBuilder(tezClient, localResources, configuration);
////		dagBuilder.build(IntWritable.class, Text.class, TextOutputFormat.class, "foo");
//	}
	
	private TezClient mockTezClient(String clientName, TezConfiguration tezConf) {
		TezClient tezClient = TezClient.create(clientName, tezConf);
		tezClient = spy(tezClient);
		when(tezClient.getClientName()).thenReturn(clientName);
		return tezClient;
	}
}
