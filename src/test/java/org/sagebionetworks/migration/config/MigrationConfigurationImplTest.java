package org.sagebionetworks.migration.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
	
	String filePath;
	
	MigrationConfigurationImpl config;
	
	String sampleKey;
	String sampleValue;
	String sourceAuthEndpoint;
	String sourceRepoEndpoint;
	String destinationAuthEndpoint;
	String destinationRepoEndPoint;
	String userName;
	String apiKey;
	
	@Before
	public void before() throws IOException {
		
		filePath = "path-to-file";
		sampleKey = "sampleKey";
		sampleValue = "sampleValue";
		sourceAuthEndpoint ="sourceAuthEndpoint";
		sourceRepoEndpoint = "souceRepoEndpoint";
		destinationAuthEndpoint ="destinationAuthEndpoint";
		destinationRepoEndPoint = "destinationRepoEndpoint";
		userName = "userName";
		apiKey = "apiKey";
		
		Properties props = new Properties();
		props.put(MigrationConfigurationImpl.KEY_CONFIG_PATH, filePath);
		props.put(sampleKey, sampleValue);
		props.put(MigrationConfigurationImpl.KEY_SOURCE_AUTHENTICATION_ENDPOINT, sourceAuthEndpoint);
		props.put(MigrationConfigurationImpl.KEY_SOURCE_REPOSITORY_ENDPOINT, sourceRepoEndpoint);
		props.put(MigrationConfigurationImpl.KEY_DESTINATION_AUTHENTICATION_ENDPOINT, destinationAuthEndpoint);
		props.put(MigrationConfigurationImpl.KEY_DESTINATION_REPOSITORY_ENDPOINT, destinationRepoEndPoint);
		props.put(MigrationConfigurationImpl.KEY_USERNAME, userName);
		props.put(MigrationConfigurationImpl.KEY_APIKEY, apiKey);
		props.put(MigrationConfigurationImpl.KEY_MAX_BACKUP_BATCHSIZE, "2");
		props.put(MigrationConfigurationImpl.KEY_MAX_RETRIES, "3");
		props.put(MigrationConfigurationImpl.KEY_BACKUP_ALIAS_TYPE, BackupAliasType.TABLE_NAME.name());
		props.put(MigrationConfigurationImpl.KEY_INCLUDE_FULL_TABLE_CHECKSUM, "true");
		
		when(mockPropertyProvider.getSystemProperties()).thenReturn(props);
		when(mockPropertyProvider.createNewProperties()).thenReturn(mockProperties);

		when(mockFileProvider.getFile(anyString())).thenReturn(mockFile);
		when(mockFileProvider.createInputStream(any(File.class))).thenReturn(mockInputStream);
		when(mockFile.exists()).thenReturn(true);
		when(mockLoggerFactory.getLogger(any())).thenReturn(mockLogger);
		
		config = new MigrationConfigurationImpl(mockLoggerFactory, mockPropertyProvider, mockFileProvider);
		verify(mockProperties).load(mockInputStream);
		verify(mockInputStream).close();
	}
	
	@Test
	public void testLoadPropertiesFromPath() throws IOException {
		// call under test
		Properties props = config.loadPropertiesFromPath(filePath);
		assertEquals(mockProperties, props);
		verify(mockProperties, times(2)).load(mockInputStream);
		verify(mockInputStream, times(2)).close();
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testLoadPropertiesFromPathDoesNotExist() throws IOException {
		when(mockFile.exists()).thenReturn(false);
		// call under test
		config.loadPropertiesFromPath(filePath);
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
		assertEquals(apiKey, info.getApiKey());
	}
	
	
	@Test
	public void testGetDestinationConnectionInfo() {
		// call under test
		SynapseConnectionInfo info = config.getDestinationConnectionInfo();
		assertNotNull(info);
		assertEquals(destinationAuthEndpoint, info.getAuthenticationEndPoint());
		assertEquals(destinationRepoEndPoint, info.getRepositoryEndPoint());
		assertEquals(userName, info.getUserName());
		assertEquals(apiKey, info.getApiKey());
	}
	
	@Test
	public void testLogConfiguration() {
		// call under test
		config.logConfiguration();
		verify(mockLogger, times(6)).info(anyString());
	}
}
