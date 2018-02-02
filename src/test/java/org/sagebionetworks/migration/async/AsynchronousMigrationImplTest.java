package org.sagebionetworks.migration.async;

import static org.mockito.Mockito.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.util.Clock;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class AsynchronousMigrationImplTest {

	@Mock
	Configuration mockConfig;
	@Mock
	DestinationJobBuilder mockJobBuilder;
	@Mock
	DestinationJobExecutor mockJobExecutor;
	@Mock
	Clock mockClock;
	@Mock
	Future<?> mockFutureOne;
	@Mock
	Future<?> mockFutureTwo;
	
	AsynchronousMigrationImpl asynchMigration;
	
	@Before
	public void before() {
		asynchMigration = new AsynchronousMigrationImpl(mockConfig, mockJobBuilder, mockJobExecutor, mockClock);
	}
	
	@Test
	public void testRemoveTerminatedJobs() throws InterruptedException, ExecutionException {
		when(mockFutureOne.isDone()).thenReturn(false);
		when(mockFutureTwo.isDone()).thenReturn(true);
		List<Future<?>> futures = Lists.newArrayList(mockFutureOne, mockFutureTwo);
		// call under test
		asynchMigration.removeTerminatedJobs(futures);
		verify(mockFutureOne, never()).get();
		verify(mockFutureTwo).get();
		assertEquals(1, futures.size());
		assertTrue(futures.contains(mockFutureOne));
	}
	
	@Test
	public void testRemoveTerminatedJobsWithAsyncMigrationException() throws InterruptedException, ExecutionException {
		when(mockFutureOne.isDone()).thenReturn(false);
		when(mockFutureTwo.isDone()).thenReturn(true);
		AsyncMigrationException exception = new AsyncMigrationException("something or another");
		// setup get failure
		doThrow(exception).when(mockFutureTwo).get();
		List<Future<?>> futures = Lists.newArrayList(mockFutureOne, mockFutureTwo);
		// call under test
		try {
			asynchMigration.removeTerminatedJobs(futures);
			fail();
		} catch (AsyncMigrationException e) {
			// expected
			assertEquals(exception, e);
		}
		// the future still must be removed from the list
		assertEquals(1, futures.size());
		assertTrue(futures.contains(mockFutureOne));
	}
	
	@Test (expected=RuntimeException.class)
	public void testRemoveTerminatedJobsWithUnknownException() throws InterruptedException, ExecutionException {
		when(mockFutureOne.isDone()).thenReturn(false);
		when(mockFutureTwo.isDone()).thenReturn(true);
		// this type will terminate migration.
		RuntimeException exception = new RuntimeException("something or another");
		// setup get failure
		doThrow(exception).when(mockFutureTwo).get();
		List<Future<?>> futures = Lists.newArrayList(mockFutureOne, mockFutureTwo);
		// call under test
		asynchMigration.removeTerminatedJobs(futures);
	}
}
