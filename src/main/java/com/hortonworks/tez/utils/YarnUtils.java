/**
 * 
 */
package com.hortonworks.tez.utils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;

import scala.actors.threadpool.Arrays;

/**
 * @author ozhurakousky
 *
 */
public class YarnUtils {
	
	@SuppressWarnings("unchecked")
	private final static Set<String> classPathExcusions = new HashSet<String>(Arrays.asList(new String[]{
			"spring-aop",
			"junit",
			"jline",
			"zookeeper",
			"curator",
			"jetty",
			"servlet",
			"netty",
			"akka",
			"paranamer",
			"concurrent",
			"metrics",
			"ant",
			"findbugs",
			"py4j",
			"tachyon",
			"pyrolite",
			"javax.transaction",
			"reflectasm",
			"minlog",
			"objenesis",
			"kryo",
			"chill",
			"mesos",
			"stream",
			"xz",
			"jsch",
			"hamcrest",
			"cglib",
			"jasper",
			"jsp",
			"jersey",
//			"javax.inject",
//			"jettison",
			"xmlenc",
			"asm",
			"colt",
			"glassfish",
			"javax.activation",
			"jul",
			"jcl",
			"config-",
			"json4s",
			"scala-reflect",
			"scala-compiler",
			"jsr305",
			"jaxb",
			"stax-api",
			"jets3t",
			"java-xmlbuilder",
			"scalap"
//			"guava"
			}));
	
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
				System.out.println("Generating application JAR: " + jarFileName);
				File jarFile = JarUtils.toJar(f, jarFileName);
				generatedJars.add(jarFile);
				f = jarFile;
			} 
			String destinationFilePath = applicationName + "/" + applicationId.getId() + "/" + f.getName();
			Path provisionedPath = new Path(fs.getHomeDirectory(), destinationFilePath);
			if (shouldProvision(provisionedPath.getName())){
				provisioinResourceToFs(fs, new Path(f.getAbsolutePath()), provisionedPath);
				provisionedPaths.add(provisionedPath);
			}
		}
		
		// perhaps need to be moved to a separate function since its Tez on Spark specific
		File rootDir = new File(System.getProperty("user.dir"));
		String[] files = rootDir.list();
		for (String fileName : files) {
			if (fileName.endsWith(".ser")){
				File serFunction = new File(rootDir, fileName);
				String destinationFilePath = applicationName + "/" + applicationId.getId() + "/" + serFunction.getName();
				Path provisionedPath = new Path(fs.getHomeDirectory(), destinationFilePath);
				provisioinResourceToFs(fs, new Path(serFunction.getAbsolutePath()), provisionedPath);
				provisionedPaths.add(provisionedPath);
				serFunction.delete();
			}
		}
		URL url = ClassLoader.getSystemClassLoader().getResource("scala/Function.class");
		String path = url.getFile();
		path = path.substring(0, path.indexOf("!"));
		
		try {
			File scalaLibLocation = new File(new URL(path).toURI());
			String destinationFilePath = applicationName + "/" + applicationId.getId() + "/" + scalaLibLocation.getName();
			Path provisionedPath = new Path(fs.getHomeDirectory(), destinationFilePath);
			provisioinResourceToFs(fs, new Path(scalaLibLocation.getAbsolutePath()), provisionedPath);
			provisionedPaths.add(provisionedPath);
		} catch (Exception e) {
			throw new RuntimeException("Failed", e);
		}
		//
		
		
		for (File generatedJar : generatedJars) {
			try {
				generatedJar.delete(); 
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return provisionedPaths.toArray(new Path[]{});
	}
	
	private static boolean shouldProvision(String path){
		for (String exclusion : classPathExcusions) {
			if (path.contains(exclusion)){
//				System.out.println("Excluding " + path);
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
			System.out.println("Provisioning '" + sourcePath + "' to " + destPath);
			fs.copyFromLocalFile(sourcePath, destPath);
		} 
		catch (IOException e) {
			throw new IllegalStateException("Failed to copy local resource " + sourcePath + " to " + destPath, e);
		}
	}
}
