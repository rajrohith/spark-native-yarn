/**
 * 
 */
package com.hortonworks.spark.tez.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.springframework.core.io.ClassPathResource;

/**
 * Utility functions related to variety of tasks to be performed via YARN
 * such as setting up LocalResource, provisioning classpath etc.
 *
 */
public class YarnUtils {
	private static final Log logger = LogFactory.getLog(YarnUtils.class);

	/**
	 * 
	 * @param fs
	 * @param appName
	 * @return
	 */
	public static Map<String, LocalResource> createLocalResources(FileSystem fs, String appName) {
		Map<String, LocalResource> localResources = provisionAndLocalizeCurrentClasspath(fs, appName);
		provisionAndLocalizeScalaLib(fs, appName, localResources);
		return localResources;
	}
	
	/**
	 * 
	 */
	private static void provisionAndLocalizeScalaLib(FileSystem fs, String appName,  Map<String, LocalResource> localResources){
		URL url = ClassLoader.getSystemClassLoader().getResource("scala/Function.class");
		String path = url.getFile();
		path = path.substring(0, path.indexOf("!"));
		
		try {
			File scalaLibLocation = new File(new URL(path).toURI());
			Path provisionedPath = YarnUtils.provisionResource(scalaLibLocation, fs, appName);
			LocalResource localResource = YarnUtils.createLocalResource(fs, provisionedPath);
			localResources.put(provisionedPath.getName(), localResource);
		} catch (Exception e) {
			throw new RuntimeException("Failed to provision Scala Library", e);
		}
	}
	
	/**
	 * 
	 */
	private static Map<String, LocalResource> provisionAndLocalizeCurrentClasspath(FileSystem fs, String appName) {
		Path[] provisionedResourcesPaths = YarnUtils.provisionClassPath(fs, appName, buildClasspathExclusions());
		Map<String, LocalResource> localResources = YarnUtils.createLocalResources(fs, provisionedResourcesPaths);

		return localResources;
	}
	
	/**
	 * 
	 * @param localResource
	 * @param fs
	 * @param applicationName
	 * @param applicationId
	 * @return
	 */
	public static Path provisionResource(File localResource, FileSystem fs, String applicationName) {
		String destinationFilePath = applicationName + "/" + localResource.getName();
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
	private static Path[] provisionClassPath(FileSystem fs, String applicationName, String[] classPathExclusions){
		List<Path> provisionedPaths = new ArrayList<Path>();
		List<File> generatedJars = new ArrayList<File>();
		URL[] classpath = ((URLClassLoader) ClassLoader.getSystemClassLoader()).getURLs();
		for (URL classpathUrl : classpath) {
			File f = new File(classpathUrl.getFile());
			if (f.isDirectory()) {
				System.out.println(" Temporarlily skipping Jar generation. PLEASE FIX!: " + f.getAbsolutePath());
//				String jarFileName = JarUtils.generateJarFileName(applicationName);
//				if (logger.isDebugEnabled()){
//					logger.debug("Generating application JAR: " + jarFileName);
//				}
//				File jarFile = JarUtils.toJar(f, jarFileName);
//				generatedJars.add(jarFile);
//				f = jarFile;
				f = null;
			} 
			if (f != null){
				String destinationFilePath = applicationName + "/" + f.getName();
				Path provisionedPath = new Path(fs.getHomeDirectory(), destinationFilePath);
				if (shouldProvision(provisionedPath.getName(), classPathExclusions)){
					provisioinResourceToFs(fs, new Path(f.getAbsolutePath()), provisionedPath);
					provisionedPaths.add(provisionedPath);
				}
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
	private static Map<String, LocalResource> createLocalResources(FileSystem fs, Path[] provisionedResourcesPaths) {
		Map<String, LocalResource> localResources = new LinkedHashMap<String, LocalResource>();
		for (Path provisionedResourcesPath : provisionedResourcesPaths) {
			LocalResource localResource = createLocalResource(fs, provisionedResourcesPath);
			localResources.put(provisionedResourcesPath.getName(), localResource);
		}
		return localResources;
	}
	
	public static FileSystem createFileSystem(TezConfiguration tezConfiguration){
		try {
			return FileSystem.get(tezConfiguration);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to access FileSystem", e);
		}
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
			if (logger.isDebugEnabled()){
				logger.debug("Provisioning '" + sourcePath + "' to " + destPath);
			}
			if (!fs.exists(destPath)){
				fs.copyFromLocalFile(sourcePath, destPath);
			}
			else {
				logger.debug("Skipping provisioning of " + destPath + " since it already exists.");
			}
		} 
		catch (IOException e) {
			logger.warn("Failed to copy local resource " + sourcePath + " to " + destPath, e);
		}
	}
	
	/**
	 * 
	 */
	private static String[] buildClasspathExclusions(){
		String[] classpathExclusions = null;
		try {
			ClassPathResource exclusionResource = new ClassPathResource("classpath_exclusions");
			if (exclusionResource.exists()){
				List<String> exclusionPatterns = new ArrayList<String>();
				BufferedReader reader = new BufferedReader(new InputStreamReader(exclusionResource.getInputStream()));
				String line;
				while ((line = reader.readLine()) != null){
					exclusionPatterns.add(line.trim());
				}
				classpathExclusions = exclusionPatterns.toArray(new String[]{});
				reader.close();
			}
			
		} catch (Exception e) {
			logger.warn("Failed to build the list of classpath exclusion. ", e);
		}
		return classpathExclusions;
	}
}
