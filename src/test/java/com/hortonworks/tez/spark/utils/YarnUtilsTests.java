package com.hortonworks.tez.spark.utils;

import java.io.File;
import java.net.URI;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.junit.Test;
import org.mockito.Mockito;

import com.hortonworks.spark.tez.utils.YarnUtils;

public class YarnUtilsTests {

	@Test
	public void validateProvisionResource() throws Exception {
		LocalFileSystem fs = new LocalFileSystem();
		fs.initialize(new URI("file:///"), new Configuration());
		File localResource = new File("src/test/java/com/hortonworks/tez/utils/foo.txt");
		Assert.assertTrue(localResource.exists());
		ApplicationId applicationId = Mockito.mock(ApplicationId.class);
		Mockito.when(applicationId.getId()).thenReturn(1);
		Path provisionedPath = YarnUtils.provisionResource(localResource, fs, "Hello", applicationId);
		File provisionedFile = new File(provisionedPath.toUri().getPath());
		Assert.assertTrue(provisionedFile.getAbsolutePath().endsWith("/Hello/1/foo.txt"));
		Assert.assertTrue(provisionedFile.exists());
		provisionedFile.delete();
		Assert.assertFalse(provisionedFile.exists());
	}
	
	@Test
	public void validateCreateLocalResource() throws Exception {
		LocalFileSystem fs = new LocalFileSystem();
		fs.initialize(new URI("file:///"), new Configuration());
		File localResource = new File("src/test/java/com/hortonworks/tez/utils/foo.txt");
		Assert.assertTrue(localResource.exists());
		ApplicationId applicationId = Mockito.mock(ApplicationId.class);
		Mockito.when(applicationId.getId()).thenReturn(1);
		Path provisionedPath = YarnUtils.provisionResource(localResource, fs, "Hello", applicationId);
		File provisionedFile = new File(provisionedPath.toUri().getPath());
		LocalResource resource = YarnUtils.createLocalResource(fs, provisionedPath);
		Assert.assertTrue(resource.getResource().getFile().endsWith("/Hello/1/foo.txt"));
		provisionedFile.delete();
		Assert.assertFalse(provisionedFile.exists());
	}
	
	@Test
	public void validateProvisionClassPath() throws Exception {
		LocalFileSystem fs = new LocalFileSystem();
		fs.initialize(new URI("file:///"), new Configuration());
		ApplicationId applicationId = Mockito.mock(ApplicationId.class);
		Mockito.when(applicationId.getId()).thenReturn(1);
		Path[] provisionedClassPath = YarnUtils.provisionClassPath(fs, "Hello", applicationId, new String[]{"junit", "mockito"});
		Assert.assertTrue(provisionedClassPath.length > 0);
		// spot check that only JARs present and that exclusions are honored
		for (Path path : provisionedClassPath) {
			File provisionedFile = new File(path.toUri().getPath());
			Assert.assertTrue(provisionedFile.exists());
			provisionedFile.delete();
			Assert.assertFalse(provisionedFile.exists());
		}
		for (Path path : provisionedClassPath) {
			if (path.getName().contains("junit") || path.getName().contains("mockito")){
				Assert.fail();
			}
			if (!path.getName().endsWith(".jar")){
				Assert.fail();
			}
		}
	}
}
