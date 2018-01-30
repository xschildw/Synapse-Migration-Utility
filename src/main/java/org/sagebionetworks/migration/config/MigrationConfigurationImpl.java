package org.sagebionetworks.migration.config;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;

import com.google.inject.Inject;

/**
 * Provides configuration information 
 */
public class MigrationConfigurationImpl implements Configuration {

	static private Logger log = LogManager.getLogger(MigrationConfigurationImpl.class);

	static final String KEY_SOURCE_REPOSITORY_ENDPOINT = "org.sagebionetworks.source.repository.endpoint";
	static final String KEY_SOURCE_AUTHENTICATION_ENDPOINT = "org.sagebionetworks.source.authentication.endpoint";
	static final String KEY_DESTINATION_REPOSITORY_ENDPOINT = "org.sagebionetworks.destination.repository.endpoint";
	static final String KEY_DESTINATION_AUTHENTICATION_ENDPOINT = "org.sagebionetworks.destination.authentication.endpoint";
	static final String KEY_CONFIG_PATH = "org.sagebionetworks.config.path";
	static final String KEY_APIKEY = "org.sagebionetworks.apikey";
	static final String KEY_USERNAME = "org.sagebionetworks.username";
	static final String KEY_MAX_THREADS = "org.sagebionetworks.max.threads";
	static final String KEY_MAX_BACKUP_BATCHSIZE = "org.sagebionetworks.max.backup.batchsize";
	static final String KEY_MIN_DELTA_RANGESIZE = "org.sagebionetworks.min.delta.rangesize";
	static final String KEY_THREAD_TIMOUT_MS = "org.sagebionetworks.worker.thread.timout.ms";
	static final String KEY_MAX_RETRIES = "org.sagebionetworks.max.retries";
	static final String KEY_THRESHOLD_PERCENTAGE = "org.sagebionetworks.full.table.migration.threshold.percentage";
	static final String KEY_BACKUP_ALIAS_TYPE = "org.sagebionetworks.backup.alias.type";
	
	SystemPropertiesProvider propProvider;
	FileProvider fileProvider;
	
	Properties systemProperties;
	
	@Inject
	public MigrationConfigurationImpl(SystemPropertiesProvider propProvider, FileProvider fileProvider) throws IOException {
		this.propProvider = propProvider;
		this.fileProvider = fileProvider;
		// load the the System properties.
		systemProperties = propProvider.getSystemProperties();
		String path = getProperty(KEY_CONFIG_PATH);
		Properties configFromPath = loadPropertiesFromPath(path);
		// add the additional config properties.
		systemProperties.putAll(configFromPath);
	}
	
	@Override
	public SynapseConnectionInfo getSourceConnectionInfo(){
		return new SynapseConnectionInfo(
					getProperty(KEY_SOURCE_AUTHENTICATION_ENDPOINT),
					getProperty(KEY_SOURCE_REPOSITORY_ENDPOINT),
					getProperty(KEY_USERNAME),
					getProperty(KEY_APIKEY)
				);
	}
	
	@Override
	public SynapseConnectionInfo getDestinationConnectionInfo(){
		return new SynapseConnectionInfo(
					getProperty(KEY_DESTINATION_AUTHENTICATION_ENDPOINT),
					getProperty(KEY_DESTINATION_REPOSITORY_ENDPOINT),
					getProperty(KEY_USERNAME),
					getProperty(KEY_APIKEY)
				);
	}
	
	@Override
	public int getMaximumNumberThreads() {
		return Integer.parseInt(getProperty(KEY_MAX_THREADS));
	}
	
	@Override
	public int getMaximumBackupBatchSize(){
		return Integer.parseInt(getProperty(KEY_MAX_BACKUP_BATCHSIZE));
	}

	@Override
	public int getMinimumDeltaRangeSize() {
		return Integer.parseInt(getProperty(KEY_MIN_DELTA_RANGESIZE));
	}
	
	@Override
	public long getWorkerTimeoutMs(){
		return Long.parseLong(getProperty(KEY_THREAD_TIMOUT_MS));
	}

	@Override
	public int getMaxRetries() {
		return Integer.parseInt(getProperty(KEY_MAX_RETRIES));
	}

	@Override
	public float getFullTableMigrationThresholdPercentage() {
		return Float.parseFloat(getProperty(KEY_THRESHOLD_PERCENTAGE));
	}

	@Override
	public BackupAliasType getBackupAliasType() {
		return BackupAliasType.valueOf(getProperty(KEY_BACKUP_ALIAS_TYPE));
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	String getProperty(String key) {
		String value = this.systemProperties.getProperty(key);
		if(value == null) {
			throw new IllegalArgumentException("Missing system property: "+key);
		}
		log.debug(key+"="+value);
		return value;
	}
	
	/**
	 * Load the the properties from the given file path.
	 * @param path
	 * @return
	 * @throws IOException
	 */
	Properties loadPropertiesFromPath(String path) throws IOException {
		File file = fileProvider.getFile(path);
		if(!file.exists()) {
			throw new IllegalArgumentException("The propery file does not exist:"+path);
		}
		InputStream fis = null;
		try{
			fis = fileProvider.createInputStream(file);
			Properties props = this.propProvider.createNewProperties();
			props.load(fis);
			return props;
		}finally{
			fis.close();
		}
	}
	
}
