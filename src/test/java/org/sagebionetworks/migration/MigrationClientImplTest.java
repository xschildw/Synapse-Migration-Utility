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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.config.Configuration;

import java.util.List;

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
	int maximumNumberOfDestinationJobs;
	MigrationClientImpl client;
	
	@Before
	public void before() {
		maxNumberRetries = 3;
		when(mockConfig.getMaxRetries()).thenReturn(maxNumberRetries);
		maximumNumberOfDestinationJobs = 2;
		when(mockConfig.getMaximumNumberOfDestinationJobs()).thenReturn(maximumNumberOfDestinationJobs);
		when(loggerFactory.getLogger(any())).thenReturn(mockLogger);
		client = new MigrationClientImpl(mockConfig, mockStackStatus, mockFullMigration, loggerFactory);
	}
	
	@Test
	public void testMigrate() {
		// call under test
		client.migrate();
		verify(mockConfig).logConfiguration();
		verify(mockStackStatus).setDestinationReadOnly();
		verify(mockFullMigration).runFullMigration(maximumNumberOfDestinationJobs);
		verify(mockStackStatus).setDestinationReadWrite();
		verify(mockLogger, atLeast(2)).info(anyString());
	}
	
	@Test
	public void testMigrateUnknownException() {
		// setup a failure
		RuntimeException unknown = new RuntimeException("an unknown exception");
		doThrow(unknown).when(mockFullMigration).runFullMigration(anyInt());
		try {
			// call under test
			client.migrate();
			fail();
		}catch(RuntimeException e) {
			assertEquals(unknown, e);
		}
		verify(mockStackStatus).setDestinationReadOnly();
		// unknown exceptions do not trigger re-tries.
		verify(mockFullMigration).runFullMigration(maximumNumberOfDestinationJobs);
		// the destination must not be set back to READ-WRITE
		verify(mockStackStatus, never()).setDestinationReadWrite();
		verify(mockLogger, never()).error(anyString(), any(Throwable.class));
	}
	
	
	@Test
	public void testMigrateAsynchException() {
		// setup a failure
		AsyncMigrationException knownException = new AsyncMigrationException("a known exception");
		ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);
		doThrow(knownException).when(mockFullMigration).runFullMigration(captor.capture());
		try {
			// call under test
			client.migrate();
			fail();
		}catch(AsyncMigrationException e) {
			assertEquals("Migration failed to run to completion without error.", e.getMessage());
		}
		verify(mockStackStatus).setDestinationReadOnly();
		// the number of attempts is controlled by the config.
		verify(mockFullMigration, times(maxNumberRetries)).runFullMigration(anyInt());
		List<Integer> maxDestinationJobsArgs = captor.getAllValues();
		assertEquals(3, maxDestinationJobsArgs.size());
		assertEquals(2, (int)maxDestinationJobsArgs.get(0));
		assertEquals(1, (int)maxDestinationJobsArgs.get(1));
		assertEquals(1, (int)maxDestinationJobsArgs.get(2));
		// the destination must not be set back to READ-WRITE
		verify(mockStackStatus, never()).setDestinationReadWrite();
		verify(mockLogger, times(maxNumberRetries)).error(anyString(), any(Throwable.class));
	}
	
	@Test
	public void testMigrateAsynchExceptionWithSuccess() {
		// setup a failure
		AsyncMigrationException knownException = new AsyncMigrationException("a known exception");
		ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);
		// exception the first time then success.
		doThrow(knownException)
		.doNothing()// success the second call
		.when(mockFullMigration).runFullMigration(captor.capture());
		// call under test
		client.migrate();
		verify(mockStackStatus).setDestinationReadOnly();
		// First attempt fails but the next succeeds
		verify(mockFullMigration, times(2)).runFullMigration(anyInt());
		List<Integer> maxDestinationJobsArgs = captor.getAllValues();
		assertEquals(2, maxDestinationJobsArgs.size());
		assertEquals(2, (int)maxDestinationJobsArgs.get(0));
		assertEquals(1, (int)maxDestinationJobsArgs.get(1));

		// the destination can be set back to READ-WRITE sine the last run was successful.
		verify(mockStackStatus).setDestinationReadWrite();
		verify(mockLogger, times(1)).error(anyString(), any(Throwable.class));
	}
}
