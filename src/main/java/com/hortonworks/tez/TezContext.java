package com.hortonworks.tez;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.mapreduce.hadoop.MRHelpers;

public class TezContext implements Closeable {
	
	private final TezConfiguration tezConfiguration;
	
	private final FileSystem fileSystem;
	
	private final Credentials credentials;
	
	private final ApplicationId applicationId;

	private final TezClient tezClient;
	
	private final String user;
	
	private final Path stagingDir;

	public TezContext(){
		this(new TezConfiguration());
	}
	
	public TezContext(TezConfiguration tezConfiguration){
		this.tezConfiguration = tezConfiguration;
		this.tezClient = new TezClient(this.tezConfiguration);
		this.tezConfiguration.set(TezConfiguration.TEZ_AM_JAVA_OPTS, MRHelpers.getMRAMJavaOpts(this.tezConfiguration));
		this.credentials = new Credentials();
		try {
			this.user = UserGroupInformation.getCurrentUser().getShortUserName();
		} catch (Exception e) {
			throw new IllegalStateException("Failed to get current user", e);
		}
		try {
			this.applicationId = this.tezClient.createApplication();
		} catch (Exception e) {
			throw new IllegalStateException("Failed to create Application Id", e);
		}
		
		try {
			this.fileSystem = FileSystem.get(this.tezConfiguration);
		} catch (Exception e) {
			throw new IllegalStateException("Failed to access FileSystem", e);
		}
		
		String stagingDirStr = Path.SEPARATOR + "user" + Path.SEPARATOR + this.user
				+ Path.SEPARATOR + ".staging" + Path.SEPARATOR + Path.SEPARATOR + applicationId.toString();
		this.tezConfiguration.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
		stagingDir = this.fileSystem.makeQualified(new Path(stagingDirStr));
	}
	
	public Credentials getCredentials() {
		return credentials;
	}

	public TezClient getTezClient(){
		return this.tezClient;
	}
	
	public TezConfiguration getTezConfiguration() {
		return tezConfiguration;
	}
	
	public FileSystem getFileSystem() {
		return fileSystem;
	}
	
	public ApplicationId getApplicationId() {
		return applicationId;
	}

	public String getUser() {
		return user;
	}
	
	public Path getStagingDir() {
		return stagingDir;
	}

	@Override
	public void close() throws IOException {
		this.fileSystem.delete(this.stagingDir, true);
	}
}
