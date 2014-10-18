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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 */
public class YarnUtilsTests {
	
	@Before
	public void before() throws Exception {
		File config = new File("conf");
		config.mkdir();
		URLClassLoader cl = (URLClassLoader) Thread.currentThread().getContextClassLoader();
		Method m = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
		m.setAccessible(true);
		m.invoke(cl, config.toURI().toURL());
	}
	
	@After
	public void after() throws Exception {
		FileUtils.deleteDirectory(new File("conf"));
	}

	@Test
	public void validateCreateLocalResourcesWithGeneratedJar() throws Exception {
		System.setProperty(TezConstants.GENERATE_JAR, "true");
		String appName = "validateCreateLocalResourcesWithGeneratedJar";
		TezConfiguration tezConfiguration = new TezConfiguration();
		FileSystem fs = FileSystem.get(tezConfiguration);
		Map<String, LocalResource> localResources = HadoopUtils.createLocalResources(fs, appName + "/" + TezConstants.CLASSPATH_PATH);
		assertTrue(localResources.size() > 1);
		boolean generatedAppJar = false;
		for (String key : localResources.keySet()) {
			if (key.startsWith("application_")){
				generatedAppJar = true;
			}
			if (key.startsWith("conf_application_")){
				fail();
			}
		}
		assertTrue(generatedAppJar);
	}
	
	@Test
	public void validateNotCreateLocalResourcesWithGeneratedJar() throws Exception {
		File config = new File("conf");
		config.mkdir();
		System.setProperty(TezConstants.GENERATE_JAR, "false");
		String appName = "validateNotCreateLocalResourcesWithGeneratedJar";
		TezConfiguration tezConfiguration = new TezConfiguration();
		FileSystem fs = FileSystem.get(tezConfiguration);
		Map<String, LocalResource> localResources = HadoopUtils.createLocalResources(fs, appName + "/" + TezConstants.CLASSPATH_PATH);
		assertTrue(localResources.size() > 1);
		boolean generatedAppJar = false;
		boolean generatedAppConfigJar = false;
		for (String key : localResources.keySet()) {
			if (key.startsWith("application_")){
				generatedAppJar = true;
				break;
			}
			if (key.startsWith("conf_application_")){
				generatedAppConfigJar = true;
			}
		}
		assertTrue(generatedAppConfigJar);
		assertFalse(generatedAppJar);
		FileUtils.deleteDirectory(config);
	}
}
