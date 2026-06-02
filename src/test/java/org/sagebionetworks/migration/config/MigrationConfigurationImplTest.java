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
import static org.sagebionetworks.migration.config.MigrationConfigurationImpl.REPO_ENDPOINT_FORMAT;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import java.util.regex.Pattern;

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
	LoggerFactory mockLoggerFactory;
	@Mock
	Logger mockLogger;
	@Mock
	AWSSecretsManager mockSecretManager;
	
	MigrationConfigurationImpl config;
	
	String sampleKey;
	String sampleValue;
	String serviceKey;
	String sourceServiceSecret;
	String destinationServiceSecret;
	Properties props;
	
	@Before
	public void before() throws IOException {
		
		sampleKey = "sampleKey";
		sampleValue = "sampleValue";
		serviceKey = "migration";
		sourceServiceSecret = "sourceKeySecret";
		destinationServiceSecret = "destinationKeySecret";
		
		props = new Properties();
		props.put(sampleKey, sampleValue);
		props.put(MigrationConfigurationImpl.KEY_SERVICE_KEY, serviceKey);
		props.put(MigrationConfigurationImpl.KEY_MAX_BACKUP_BATCHSIZE, "2");
		props.put(MigrationConfigurationImpl.KEY_MAX_RETRIES, "3");
		props.put(MigrationConfigurationImpl.KEY_BACKUP_ALIAS_TYPE, BackupAliasType.TABLE_NAME.name());
		props.put(MigrationConfigurationImpl.KEY_INCLUDE_FULL_TABLE_CHECKSUM, "true");
		props.put(MigrationConfigurationImpl.KEY_DELAY_BEFORE_START_MS, "30000");
		props.put(MigrationConfigurationImpl.KEY_THREAD_TIMOUT_MS, "100000000");

		when(mockPropertyProvider.getSystemProperties()).thenReturn(props);

		when(mockLoggerFactory.getLogger(any())).thenReturn(mockLogger);
		
		config = new MigrationConfigurationImpl(mockLoggerFactory, mockPropertyProvider, mockFileProvider, mockSecretManager);
	}
	

	@Test
	public void testRepoEndpointFormat() throws MalformedURLException {
		String endpoint = String.format(REPO_ENDPOINT_FORMAT, "stackType", "stack", "endpointType");
		// This will fail if the resulting endpoint is not a valid URL
		URL url = new URL(endpoint);
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
	public void testLogConfiguration() {
		props.put(MigrationConfigurationImpl.KEY_STACK, "dev");
		when(mockSecretManager
				.getSecretValue(new GetSecretValueRequest().withSecretId(MigrationConfigurationImpl.KEY_SOURCE_SERVICE_SECRET)))
				.thenReturn(new GetSecretValueResult().withSecretString(sourceServiceSecret));
		when(mockSecretManager
				.getSecretValue(new GetSecretValueRequest().withSecretId(MigrationConfigurationImpl.KEY_DESTINATION_SERVICE_SECRET)))
				.thenReturn(new GetSecretValueResult().withSecretString(destinationServiceSecret));

		// call under test
		config.logConfiguration();
		verify(mockLogger, times(8)).info(anyString());
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
	@Test
	public void testGetConnectionInfoProd() {
		when(mockSecretManager
				.getSecretValue(new GetSecretValueRequest().withSecretId(MigrationConfigurationImpl.KEY_SOURCE_SERVICE_SECRET)))
				.thenReturn(new GetSecretValueResult().withSecretString(sourceServiceSecret));
		when(mockSecretManager
				.getSecretValue(new GetSecretValueRequest().withSecretId(MigrationConfigurationImpl.KEY_DESTINATION_SERVICE_SECRET)))
				.thenReturn(new GetSecretValueResult().withSecretString(destinationServiceSecret));
		props.put(MigrationConfigurationImpl.KEY_STACK, "prod");
		// source
		SynapseConnectionInfo connInfo = config.getSourceConnectionInfo();
		assertNotNull(connInfo);
		assertEquals("https://repo-prod.prod.sagebase.org/repo/v1", connInfo.getRepositoryEndPoint());
		assertEquals("https://repo-prod.prod.sagebase.org/auth/v1", connInfo.getAuthenticationEndPoint());
		assertEquals(serviceKey, connInfo.getServiceKey());
		assertEquals(sourceServiceSecret, connInfo.getServiceSecret());
		// destination
		connInfo = config.getDestinationConnectionInfo();
		assertNotNull(connInfo);
		assertEquals("https://repo-staging.prod.sagebase.org/repo/v1", connInfo.getRepositoryEndPoint());
		assertEquals("https://repo-staging.prod.sagebase.org/auth/v1", connInfo.getAuthenticationEndPoint());
		assertEquals(serviceKey, connInfo.getServiceKey());
		assertEquals(destinationServiceSecret, connInfo.getServiceSecret());
	}

	@Test
	public void testGetConnectionInfoDev() {
		when(mockSecretManager
				.getSecretValue(new GetSecretValueRequest().withSecretId(MigrationConfigurationImpl.KEY_SOURCE_SERVICE_SECRET)))
				.thenReturn(new GetSecretValueResult().withSecretString(sourceServiceSecret));
		when(mockSecretManager
				.getSecretValue(new GetSecretValueRequest().withSecretId(MigrationConfigurationImpl.KEY_DESTINATION_SERVICE_SECRET)))
				.thenReturn(new GetSecretValueResult().withSecretString(destinationServiceSecret));
		props.put(MigrationConfigurationImpl.KEY_STACK, "dev");

		SynapseConnectionInfo connInfo = config.getSourceConnectionInfo();
		assertNotNull(connInfo);
		assertEquals("https://repo-prod.dev.sagebase.org/repo/v1", connInfo.getRepositoryEndPoint());
		assertEquals("https://repo-prod.dev.sagebase.org/auth/v1", connInfo.getAuthenticationEndPoint());
		assertEquals(serviceKey, connInfo.getServiceKey());
		assertEquals(sourceServiceSecret, connInfo.getServiceSecret());
		// destination
		connInfo = config.getDestinationConnectionInfo();
		assertNotNull(connInfo);
		assertEquals("https://repo-staging.dev.sagebase.org/repo/v1", connInfo.getRepositoryEndPoint());
		assertEquals("https://repo-staging.dev.sagebase.org/auth/v1", connInfo.getAuthenticationEndPoint());
		assertEquals(serviceKey, connInfo.getServiceKey());
		assertEquals(destinationServiceSecret, connInfo.getServiceSecret());
	}

	@Test
	public void testGetDestinationConnectionInfoTstProd() {
		when(mockSecretManager
				.getSecretValue(new GetSecretValueRequest().withSecretId(MigrationConfigurationImpl.KEY_DESTINATION_SERVICE_SECRET)))
				.thenReturn(new GetSecretValueResult().withSecretString(destinationServiceSecret));
		props.put(MigrationConfigurationImpl.KEY_STACK, "prod");
		props.put(MigrationConfigurationImpl.KEY_DESTINATION_STACK_TYPE, "tst");

		SynapseConnectionInfo connInfo = config.getDestinationConnectionInfo();
		assertNotNull(connInfo);
		assertEquals("https://repo-tst.prod.sagebase.org/repo/v1", connInfo.getRepositoryEndPoint());
		assertEquals("https://repo-tst.prod.sagebase.org/auth/v1", connInfo.getAuthenticationEndPoint());
		assertEquals(serviceKey, connInfo.getServiceKey());
		assertEquals(destinationServiceSecret, connInfo.getServiceSecret());
	}

	@Test
	public void testGetDestinationConnectionInfoTstDev() {
		when(mockSecretManager
				.getSecretValue(new GetSecretValueRequest().withSecretId(MigrationConfigurationImpl.KEY_DESTINATION_SERVICE_SECRET)))
				.thenReturn(new GetSecretValueResult().withSecretString(destinationServiceSecret));
		props.put(MigrationConfigurationImpl.KEY_STACK, "dev");
		props.put(MigrationConfigurationImpl.KEY_DESTINATION_STACK_TYPE, "TST");

		SynapseConnectionInfo connInfo = config.getDestinationConnectionInfo();
		assertNotNull(connInfo);
		assertEquals("https://repo-tst.dev.sagebase.org/repo/v1", connInfo.getRepositoryEndPoint());
		assertEquals("https://repo-tst.dev.sagebase.org/auth/v1", connInfo.getAuthenticationEndPoint());
		assertEquals(serviceKey, connInfo.getServiceKey());
		assertEquals(destinationServiceSecret, connInfo.getServiceSecret());
	}

	@Test (expected=IllegalArgumentException.class)
	public void testGetDestinationConnectionInfoUnsupportedDestinationStackType() {
		props.put(MigrationConfigurationImpl.KEY_STACK, "dev");
		props.put(MigrationConfigurationImpl.KEY_DESTINATION_STACK_TYPE, "prod");

		// call under test
		config.getDestinationConnectionInfo();
	}

}
