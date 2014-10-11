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
package org.apache.spark.tez.test.utils;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * 
 */
public class TezClientMocker {

	public static TezClient noOpTezClientWithSuccessfullSubmit(String applicationName){
		try {
			TezClient tezClient = mock(TezClient.class);
			when(tezClient.getClientName()).thenReturn(applicationName);
		    doAnswer(new Answer<Void>() {
				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					return null;
				}
			}).when(tezClient).waitTillReady();
			
		    doAnswer(new Answer<DAGClient>() {
				@SuppressWarnings("unchecked")
				@Override
				public DAGClient answer(InvocationOnMock invocation) throws Throwable {
					DAGClient client = mock(DAGClient.class);
					DAGStatus status = mock(DAGStatus.class);
					when(status.getState()).thenReturn(DAGStatus.State.SUCCEEDED);
					when(client.waitForCompletionWithStatusUpdates(Mockito.anySet())).thenReturn(status);
					return client;
				}
			}).when(tezClient).submitDAG(Mockito.any(DAG.class));
			
			return tezClient;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
}
