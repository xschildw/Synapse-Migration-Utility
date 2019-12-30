package org.sagebionetworks.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.*;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.config.Configuration;

@RunWith(MockitoJUnitRunner.class)
public class MigrationClientImplTest {

	@Mock
	LoggerFactory loggerFactory;
	@Mock
	Logger mockLogger;
	@Mock
	Configuration mockConfig;
	@Mock
	StackStatusService mockStackStatus;
	@Mock
	FullMigration mockFullMigration;
	
	int maxNumberRetries;
	MigrationClientImpl client;
	
	@Before
	public void before() {
		maxNumberRetries = 3;
		when(mockConfig.getMaxRetries()).thenReturn(maxNumberRetries);
		when(loggerFactory.getLogger(any())).thenReturn(mockLogger);
		when(mockConfig.remainInReadOnlyAfterMigration()).thenReturn(false);
		client = new MigrationClientImpl(mockConfig, mockStackStatus, mockFullMigration, loggerFactory);
	}
	
	@Test
	public void testMigrate() {
		// call under test
		client.migrate();
		verify(mockConfig).logConfiguration();
		verify(mockStackStatus).setDestinationReadOnly();
		verify(mockStackStatus).setDestinationReadWrite();
		verify(mockFullMigration).runFullMigration();
		verify(mockLogger, atLeast(3)).info(anyString());
	}

	@Test
	public void testMigrateDestinationRemainReadOnly() {
		when(mockConfig.remainInReadOnlyAfterMigration()).thenReturn(true);
		// call under test
		client.migrate();
		verify(mockConfig).logConfiguration();
		verify(mockStackStatus).setDestinationReadOnly();
		verify(mockStackStatus, never()).setDestinationReadWrite();
		verify(mockFullMigration).runFullMigration();
		verify(mockLogger, atLeast(3)).info(anyString());
	}

	@Test
	public void testMigrateUnknownException() {
		// setup a failure
		RuntimeException unknown = new RuntimeException("an unknown exception");
		doThrow(unknown).when(mockFullMigration).runFullMigration();
		try {
			// call under test
			client.migrate();
			fail();
		}catch(RuntimeException e) {
			assertEquals(unknown, e);
		}
		// unknown exceptions do not trigger re-tries.
		verify(mockFullMigration).runFullMigration();
		// destination was set to READ-ONLY
		verify(mockStackStatus).setDestinationReadOnly();
		// the destination must not be set back to READ-WRITE
		verify(mockStackStatus, never()).setDestinationReadWrite();
		verify(mockLogger, never()).error(anyString(), any(Throwable.class));
	}
	
	
	@Test
	public void testMigrateAsynchException() {
		// setup a failure
		AsyncMigrationException knownException = new AsyncMigrationException("a known exception");
		doThrow(knownException).when(mockFullMigration).runFullMigration();
		try {
			// call under test
			client.migrate();
			fail();
		}catch(AsyncMigrationException e) {
			assertEquals("Migration failed to run to completion without error.", e.getMessage());
		}
		// the number of attempts is controlled by the config.
		verify(mockFullMigration, times(maxNumberRetries)).runFullMigration();
		// destination was set to READ-ONLY
		verify(mockStackStatus).setDestinationReadOnly();
		// the destination must not be set back to READ-WRITE
		verify(mockStackStatus, never()).setDestinationReadWrite();
		verify(mockLogger, times(maxNumberRetries)).error(anyString(), any(Throwable.class));
	}
	
	@Test
	public void testMigrateAsynchExceptionWithSuccess() {
		// setup a failure
		AsyncMigrationException knownException = new AsyncMigrationException("a known exception");
		// exception the first time then success.
		doThrow(knownException)
		.doNothing()// success the second call
		.when(mockFullMigration).runFullMigration();
		// call under test
		client.migrate();
		// First attempt fails but the next succeeds
		verify(mockFullMigration, times(2)).runFullMigration();
		verify(mockLogger, times(1)).error(anyString(), any(Throwable.class));
		// destination was set to READ-ONLY
		verify(mockStackStatus).setDestinationReadOnly();
		// was set back to READ-WRITE
		verify(mockStackStatus).setDestinationReadWrite();

	}
}
