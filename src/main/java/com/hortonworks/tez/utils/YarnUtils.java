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
	
	/**
	 * Will provision current classpath to YARN and return an array of 
	 * {@link Path}s representing provisioned resources
	 * 
	 * @return
	 */
	public static Path[] provisionClassPath(FileSystem fs, String applicationName, ApplicationId applicationId){
		List<Path> provisionedPaths = new ArrayList<Path>();
		List<File> generatedJars = new ArrayList<File>();
		URL[] classpath = ((URLClassLoader) ClassLoader.getSystemClassLoader()).getURLs();
		for (URL classpathUrl : classpath) {
			File f = new File(classpathUrl.getFile());
			if (f.isDirectory()) {
				String jarFileName = JarUtils.generateJarFileName(applicationName);
				System.out.println("Creating JAR: " + jarFileName);
				File jarFile = JarUtils.toJar(f, jarFileName);
				generatedJars.add(jarFile);
				f = jarFile;
			} 
			String destinationFilePath = applicationName + "/" + applicationId.getId() + "/" + f.getName();
			Path provisionedPath = new Path(fs.getHomeDirectory(), destinationFilePath);
			provisioinResourceToFs(fs, new Path(f.getAbsolutePath()), provisionedPath);
			provisionedPaths.add(provisionedPath);
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
	 * @param fs
	 * @param sourcePath
	 * @param destPath
	 * @param appId
	 * @param applicationName
	 * @return
	 */
	public static Map<String, LocalResource> createLocalResources(FileSystem fs, Path[] provisionedResourcesPaths) {
		Map<String, LocalResource> localResources = new LinkedHashMap<String, LocalResource>();
		for (Path path : provisionedResourcesPaths) {
			try {
				FileStatus scFileStatus = fs.getFileStatus(path);
				LocalResource localResource = LocalResource.newInstance(
						ConverterUtils.getYarnUrlFromURI(path.toUri()),
						LocalResourceType.FILE,
						LocalResourceVisibility.APPLICATION, scFileStatus.getLen(),
						scFileStatus.getModificationTime());
				localResources.put(path.getName(), localResource);
			} 
			catch (IOException e) {
				throw new IllegalStateException("Failed to communicate with FileSystem: " + fs, e);
			}
		}
		return localResources;
	}
	
	/**
	 * 
	 * @param fs
	 * @param sourcePath
	 * @param destPath
	 */
	public static void provisioinResourceToFs(FileSystem fs, Path sourcePath, Path destPath) {
		try {
			System.out.println("Copying '" + sourcePath + "' to " + destPath);
			fs.copyFromLocalFile(sourcePath, destPath);
		} 
		catch (IOException e) {
			throw new IllegalStateException("Failed to copy local resource " + sourcePath + " to " + destPath, e);
		}
	}
}
