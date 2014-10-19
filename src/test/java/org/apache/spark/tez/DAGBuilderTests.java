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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.spark.tez.test.utils.Instrumentable;
import org.apache.spark.tez.utils.ReflectionUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Test;

/**
 *
 */
public class DAGBuilderTests extends Instrumentable {

	@SuppressWarnings("unchecked")
	@Test(expected=IllegalStateException.class)
	public void failWithappNameNull() {
		new DAGBuilder(null, mock(Map.class), mock(Configuration.class));
	}
	
	@Test(expected=IllegalStateException.class)
	public void failWithLocalResourcesMapNull() {
		new DAGBuilder("foo", null, mock(Configuration.class));
	}
	
	@SuppressWarnings("unchecked")
	@Test(expected=IllegalStateException.class)
	public void failWithConfigurationNull() {
		new DAGBuilder("foo", mock(Map.class), null);
	}
	
	@Test
	public void validateInitialization() {
		String appName = "foo";
		TezConfiguration configuration = new TezConfiguration();
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		DAGBuilder dagBuilder = new DAGBuilder(appName, localResources, configuration);
		
		assertSame(configuration, ReflectionUtils.getFieldValue(dagBuilder, "tezConfiguration"));
		assertSame(localResources, ReflectionUtils.getFieldValue(dagBuilder, "localResources"));
		assertNotNull(ReflectionUtils.getFieldValue(dagBuilder, "dag"));
	}
	
	@Test(expected=IllegalStateException.class)
	public void validateFailureWithMissingKeyType() {
		TezConfiguration configuration = new TezConfiguration();
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		DAGBuilder dagBuilder = new DAGBuilder("foo", localResources, configuration);
		dagBuilder.build(null, Text.class, TextOutputFormat.class, "foo");
	}
	
	@Test(expected=IllegalStateException.class)
	public void validateFailureWithMissingValueType() {
		TezConfiguration configuration = new TezConfiguration();
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		DAGBuilder dagBuilder = new DAGBuilder("foo", localResources, configuration);
		dagBuilder.build(IntWritable.class, null, TextOutputFormat.class, "foo");
	}
	
	@Test(expected=IllegalStateException.class)
	public void validateFailureWithMissingOutputFormat() {
		TezConfiguration configuration = new TezConfiguration();
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		
		DAGBuilder dagBuilder = new DAGBuilder("foo", localResources, configuration);
		dagBuilder.build(IntWritable.class, Text.class, null, "foo");
	}
	
	@Test(expected=IllegalStateException.class)
	public void validateFailureWithMissingOutputPath() {
		TezConfiguration configuration = new TezConfiguration();
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

		DAGBuilder dagBuilder = new DAGBuilder("foo", localResources, configuration);
		dagBuilder.build(IntWritable.class, Text.class, TextOutputFormat.class, null);
	}
	
	@Test(expected=IllegalStateException.class)
	public void validateFailureWithMissingVertexesBuild() {
		TezConfiguration configuration = new TezConfiguration();
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		DAGBuilder dagBuilder = new DAGBuilder("foo", localResources, configuration);
		dagBuilder.build(IntWritable.class, Text.class, TextOutputFormat.class, "foo");
	}
}
