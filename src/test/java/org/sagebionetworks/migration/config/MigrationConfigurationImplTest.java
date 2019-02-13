package org.sagebionetworks.migration.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.LoggerFactory;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;

@RunWith(MockitoJUnitRunner.class)
public class MigrationConfigurationImplTest {

	@Mock
	SystemPropertiesProvider mockPropertyProvider;
	@Mock
	FileProvider mockFileProvider;
	@Mock
	File mockFile;
	@Mock
	InputStream mockInputStream;
	@Mock
	Properties mockProperties;
	@Mock
	LoggerFactory mockLoggerFactory;
	@Mock
	Logger mockLogger;
	@Mock
	AWSSecretsManager mockSecretManager;
	
	MigrationConfigurationImpl config;
	
	String sampleKey;
	String sampleValue;
	String sourceAuthEndpoint;
	String sourceRepoEndpoint;
	String destinationAuthEndpoint;
	String destinationRepoEndPoint;
	String userName;
	String sourceApiKey;
	String destinationApiKey;
	Properties props;
	
	@Before
	public void before() throws IOException {
		
		sampleKey = "sampleKey";
		sampleValue = "sampleValue";
		sourceAuthEndpoint ="sourceAuthEndpoint";
		sourceRepoEndpoint = "souceRepoEndpoint";
		destinationAuthEndpoint ="destinationAuthEndpoint";
		destinationRepoEndPoint = "destinationRepoEndpoint";
		userName = "userName";
		sourceApiKey = "sourceKeySecret";
		destinationApiKey = "destinationKeySecret";
		
		props = new Properties();
		props.put(sampleKey, sampleValue);
		props.put(MigrationConfigurationImpl.KEY_SOURCE_AUTHENTICATION_ENDPOINT, sourceAuthEndpoint);
		props.put(MigrationConfigurationImpl.KEY_SOURCE_REPOSITORY_ENDPOINT, sourceRepoEndpoint);
		props.put(MigrationConfigurationImpl.KEY_DESTINATION_AUTHENTICATION_ENDPOINT, destinationAuthEndpoint);
		props.put(MigrationConfigurationImpl.KEY_DESTINATION_REPOSITORY_ENDPOINT, destinationRepoEndPoint);
		props.put(MigrationConfigurationImpl.KEY_USERNAME, userName);
		props.put(MigrationConfigurationImpl.KEY_MAX_BACKUP_BATCHSIZE, "2");
		props.put(MigrationConfigurationImpl.KEY_MAX_RETRIES, "3");
		props.put(MigrationConfigurationImpl.KEY_BACKUP_ALIAS_TYPE, BackupAliasType.TABLE_NAME.name());
		props.put(MigrationConfigurationImpl.KEY_INCLUDE_FULL_TABLE_CHECKSUM, "true");
		props.put(MigrationConfigurationImpl.KEY_DELAY_BEFORE_START_MS, "30000");
		props.put(MigrationConfigurationImpl.KEY_THREAD_TIMOUT_MS, "100000000");
		props.put(MigrationConfigurationImpl.KEY_DESTINATION_ROW_COUNT_TO_IGNORE, "1000");
		
		when(mockPropertyProvider.getSystemProperties()).thenReturn(props);
		when(mockPropertyProvider.createNewProperties()).thenReturn(mockProperties);

		when(mockFileProvider.getFile(anyString())).thenReturn(mockFile);
		when(mockFileProvider.createInputStream(any(File.class))).thenReturn(mockInputStream);
		when(mockFile.exists()).thenReturn(true);
		when(mockLoggerFactory.getLogger(any())).thenReturn(mockLogger);
		
		when(mockSecretManager
				.getSecretValue(new GetSecretValueRequest().withSecretId(MigrationConfigurationImpl.KEY_SOURCE_APIKEY)))
						.thenReturn(new GetSecretValueResult().withSecretString(sourceApiKey));
		when(mockSecretManager
				.getSecretValue(new GetSecretValueRequest().withSecretId(MigrationConfigurationImpl.KEY_DESTINATION_APIKEY)))
						.thenReturn(new GetSecretValueResult().withSecretString(destinationApiKey));
		
		config = new MigrationConfigurationImpl(mockLoggerFactory, mockPropertyProvider, mockFileProvider, mockSecretManager);
	}
	
	@Test
	public void testGetProperty() {
		// call under test
		String value = config.getProperty(sampleKey);
		assertEquals(sampleValue, value);
	}
	
	
	@Test (expected=IllegalArgumentException.class)
	public void testGetPropertyDoesNotExist() {
		// call under test
		config.getProperty("doesNotExist");
	}
	
	@Test
	public void testGetSourceConnectionInfo() {
		// call under test
		SynapseConnectionInfo info = config.getSourceConnectionInfo();
		assertNotNull(info);
		assertEquals(sourceAuthEndpoint, info.getAuthenticationEndPoint());
		assertEquals(sourceRepoEndpoint, info.getRepositoryEndPoint());
		assertEquals(userName, info.getUserName());
		assertEquals(sourceApiKey, info.getApiKey());
	}
	
	
	@Test
	public void testGetDestinationConnectionInfo() {
		// call under test
		SynapseConnectionInfo info = config.getDestinationConnectionInfo();
		assertNotNull(info);
		assertEquals(destinationAuthEndpoint, info.getAuthenticationEndPoint());
		assertEquals(destinationRepoEndPoint, info.getRepositoryEndPoint());
		assertEquals(userName, info.getUserName());
		assertEquals(destinationApiKey, info.getApiKey());
	}
	
	@Test
	public void testLogConfiguration() {
		// call under test
		config.logConfiguration();
		verify(mockLogger, times(9)).info(anyString());
	}
	
	@Test
	public void testRemainInReadOnlyAfterMigrationDeafult() {
		// by default should return false.
		assertFalse(config.remainInReadOnlyAfterMigration());
	}
	
	@Test
	public void testRemainInReadOnlyAfterMigrationSet() {
		// set the value
		props.put(MigrationConfigurationImpl.KEY_REMAIN_READ_ONLY_MODE, "true");
		assertTrue(config.remainInReadOnlyAfterMigration());
	}
}
