/**
 * 
 */
package com.hortonworks.tez.utils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * @author ozhurakousky
 *
 */
public class YarnUtils {
	
	private static final Log logger = LogFactory.getLog(YarnUtils.class);
	
	/**
	 * 
	 * @param localResource
	 * @param fs
	 * @param applicationName
	 * @param applicationId
	 * @return
	 */
	public static Path provisionResource(File localResource, FileSystem fs, String applicationName, ApplicationId applicationId) {
		String destinationFilePath = applicationName + "/" + applicationId.getId() + "/" + localResource.getName();
		Path provisionedPath = new Path(fs.getHomeDirectory(), destinationFilePath);
		provisioinResourceToFs(fs, new Path(localResource.getAbsolutePath()), provisionedPath);
		return provisionedPath;
	}
	
	/**
	 * Will provision current classpath to YARN and return an array of 
	 * {@link Path}s representing provisioned resources
	 * 
	 * @return
	 */
	public static Path[] provisionClassPath(FileSystem fs, String applicationName, ApplicationId applicationId, String[] classPathExclusions){
		List<Path> provisionedPaths = new ArrayList<Path>();
		List<File> generatedJars = new ArrayList<File>();
		URL[] classpath = ((URLClassLoader) ClassLoader.getSystemClassLoader()).getURLs();
		for (URL classpathUrl : classpath) {
			File f = new File(classpathUrl.getFile());
			if (f.isDirectory()) {
				String jarFileName = JarUtils.generateJarFileName(applicationName);
				System.out.println("Generating application JAR: " + jarFileName);
				File jarFile = JarUtils.toJar(f, jarFileName);
				generatedJars.add(jarFile);
				f = jarFile;
			} 
			String destinationFilePath = applicationName + "/" + applicationId.getId() + "/" + f.getName();
			Path provisionedPath = new Path(fs.getHomeDirectory(), destinationFilePath);
			if (shouldProvision(provisionedPath.getName(), classPathExclusions)){
				provisioinResourceToFs(fs, new Path(f.getAbsolutePath()), provisionedPath);
				provisionedPaths.add(provisionedPath);
			}
		}
		
		for (File generatedJar : generatedJars) {
			try {
				generatedJar.delete(); 
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return provisionedPaths.toArray(new Path[]{});
	}
	
	/**
	 * 
	 * @param path
	 * @param classPathExclusions
	 * @return
	 */
	private static boolean shouldProvision(String path, String[] classPathExclusions){
		for (String exclusion : classPathExclusions) {
			if (path.contains(exclusion) || !path.endsWith(".jar")){
				if (logger.isDebugEnabled()){
					logger.debug("Excluding resource: " + path);
				}
				return false;
			}
		}
		return true;
	}

	/**
	 * 
	 * @param fs
	 * @param sourcePath
	 * @param destPath
	 * @param appId
	 * @param applicationName
	 * @return
	 */
	public static Map<String, LocalResource> createLocalResources(FileSystem fs, Path[] provisionedResourcesPaths) {
		Map<String, LocalResource> localResources = new LinkedHashMap<String, LocalResource>();
		for (Path provisionedResourcesPath : provisionedResourcesPaths) {
			LocalResource localResource = createLocalResource(fs, provisionedResourcesPath);
			localResources.put(provisionedResourcesPath.getName(), localResource);
		}
		return localResources;
	}
	
	/**
	 * 
	 * @param fs
	 * @param provisionedResourcePath
	 * @return
	 */
	public static LocalResource createLocalResource(FileSystem fs, Path provisionedResourcePath){
		try {
			FileStatus scFileStatus = fs.getFileStatus(provisionedResourcePath);
			LocalResource localResource = LocalResource.newInstance(
					ConverterUtils.getYarnUrlFromURI(provisionedResourcePath.toUri()),
					LocalResourceType.FILE,
					LocalResourceVisibility.APPLICATION, scFileStatus.getLen(),
					scFileStatus.getModificationTime());
			return localResource;
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to communicate with FileSystem while creating LocalResource: " + fs, e);
		}
	}
	/**
	 * 
	 * @param fs
	 * @param sourcePath
	 * @param destPath
	 */
	private static void provisioinResourceToFs(FileSystem fs, Path sourcePath, Path destPath) {
		try {
			System.out.println("Provisioning '" + sourcePath + "' to " + destPath);
			fs.copyFromLocalFile(sourcePath, destPath);
		} 
		catch (IOException e) {
			logger.warn("Failed to copy local resource " + sourcePath + " to " + destPath, e);
		}
	}
}
