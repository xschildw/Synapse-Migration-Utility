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
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.repo.model.migration.MigrationType;
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
	Future mockFutureOne;
	@Mock
	Future mockFutureTwo;
	@Mock
	Future mockFutureThree;

	List<TypeToMigrateMetadata> primaryTypes;
	RestoreDestinationJob jobOne;
	RestoreDestinationJob jobTwo;
	RestoreDestinationJob jobThree;
	List<DestinationJob> destinationJobs;

	AsynchronousMigrationImpl asynchMigration;

	@Before
	public void before() {
		jobOne = new RestoreDestinationJob(MigrationType.NODE, "someKey1");
		jobTwo = new RestoreDestinationJob(MigrationType.NODE, "someKey2");
		jobThree = new RestoreDestinationJob(MigrationType.NODE, "someKey3");
		destinationJobs = Lists.newArrayList(jobOne,jobTwo, jobThree);
		
		TypeToMigrateMetadata toMigrate = new TypeToMigrateMetadata();
		toMigrate.setType(MigrationType.NODE);
		toMigrate.setSrcMinId(1L);
		toMigrate.setSrcMaxId(99L);
		toMigrate.setSrcCount(98L);
		toMigrate.setDestMinId(1L);
		toMigrate.setDestMaxId(4L);
		toMigrate.setDestCount(3L);
		primaryTypes = Lists.newArrayList(toMigrate);
		
		when(mockJobBuilder.buildDestinationJobs(primaryTypes)).thenReturn(destinationJobs.iterator());
		
		when(mockJobExecutor.startDestinationJob(jobOne)).thenReturn(mockFutureOne);
		when(mockJobExecutor.startDestinationJob(jobTwo)).thenReturn(mockFutureTwo);
		when(mockJobExecutor.startDestinationJob(jobThree)).thenReturn(mockFutureThree);
		
		asynchMigration = new AsynchronousMigrationImpl(mockConfig, mockJobBuilder, mockJobExecutor, mockClock);
	}
	
	@Test
	public void testGetJobResults() throws Exception {
		asynchMigration.getJobResults(mockFutureOne);
		verify(mockFutureOne).get();
	}
	
	@Test (expected=AsyncMigrationException.class)
	public void testGetJobResultsAsyncMigrationException() throws Exception {
		AsyncMigrationException exception = new AsyncMigrationException("something to retry");
		when(mockFutureOne.get()).thenThrow(exception);
		// call under test
		asynchMigration.getJobResults(mockFutureOne);
	}
	
	@Test (expected=RuntimeException.class)
	public void testGetJobResultsUnknownException() throws Exception {
		IllegalArgumentException exception = new IllegalArgumentException("something to retry");
		when(mockFutureOne.get()).thenThrow(exception);
		// call under test
		asynchMigration.getJobResults(mockFutureOne);
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
	public void testRemoveTerminatedJobsWithMultipleAsyncMigrationException() throws InterruptedException, ExecutionException {
		when(mockFutureOne.isDone()).thenReturn(true);
		when(mockFutureTwo.isDone()).thenReturn(true);
		when(mockFutureThree.isDone()).thenReturn(false);
		AsyncMigrationException exceptionOne = new AsyncMigrationException("one");
		AsyncMigrationException exceptionTwo = new AsyncMigrationException("two");
		// setup get failure
		doThrow(exceptionOne).when(mockFutureOne).get();
		doThrow(exceptionTwo).when(mockFutureTwo).get();
		List<Future<?>> futures = Lists.newArrayList(mockFutureOne, mockFutureTwo, mockFutureThree);
		// call under test
		try {
			asynchMigration.removeTerminatedJobs(futures);
			fail();
		} catch (AsyncMigrationException e) {
			// should match the second exception thrown
			assertEquals(exceptionTwo, e);
		}
		// both of the failed jobs should be removed.
		assertEquals(1, futures.size());
		assertTrue(futures.contains(mockFutureThree));
	}
	
	@Test
	public void testRemoveTerminatedJobsWithMultipleAsyncMigrationExceptionIsDone() throws InterruptedException, ExecutionException {
		when(mockFutureOne.isDone()).thenReturn(true);
		when(mockFutureTwo.isDone()).thenReturn(true);
		when(mockFutureThree.isDone()).thenReturn(false);
		AsyncMigrationException exceptionOne = new AsyncMigrationException("one");
		AsyncMigrationException exceptionTwo = new AsyncMigrationException("two");
		// setup get failure
		doThrow(exceptionOne).when(mockFutureOne).isDone();
		doThrow(exceptionTwo).when(mockFutureTwo).isDone();
		List<Future<?>> futures = Lists.newArrayList(mockFutureOne, mockFutureTwo, mockFutureThree);
		// call under test
		try {
			asynchMigration.removeTerminatedJobs(futures);
			fail();
		} catch (AsyncMigrationException e) {
			// should match the second exception thrown
			assertEquals(exceptionTwo, e);
		}
		// both of the failed jobs should be removed.
		assertEquals(1, futures.size());
		assertTrue(futures.contains(mockFutureThree));
	}
	
	@Test
	public void testSleep() throws InterruptedException {
		// call under test
		asynchMigration.sleep();
		verify(mockClock).sleep(AsynchronousMigrationImpl.SLEEP_TIME_MS);
	}
	
	@Test (expected=RuntimeException.class)
	public void testSleepInterrupt() throws InterruptedException {
		InterruptedException interrupted = new InterruptedException("an interrupt");
		doThrow(interrupted).when(mockClock).sleep(any(Long.class));
		// call under test
		asynchMigration.sleep();
	}
	
	@Test
	public void testWaitForJobsToTerminate() throws Exception {
		List<Future<?>> futures = Lists.newArrayList(mockFutureOne, mockFutureTwo, mockFutureThree);
		// call under test
		asynchMigration.waitForJobsToTerminate(futures);
		// should wait for all jobs to finish
		verify(mockFutureOne).get();
		verify(mockFutureTwo).get();
		verify(mockFutureThree).get();
	}
	
	@Test
	public void testWaitForJobsToTerminateMultipleExceptions() throws Exception {
		List<Future<?>> futures = Lists.newArrayList(mockFutureOne, mockFutureTwo, mockFutureThree);
		AsyncMigrationException exceptionOne = new AsyncMigrationException("one");
		AsyncMigrationException exceptionTwo = new AsyncMigrationException("two");
		// setup get failures
		doThrow(exceptionOne).when(mockFutureOne).get();
		doThrow(exceptionTwo).when(mockFutureTwo).get();
		// call under test
		try {
			asynchMigration.waitForJobsToTerminate(futures);
		} catch (Exception e) {
			assertEquals(exceptionTwo, e);
		}
		// should wait for all jobs to finish
		verify(mockFutureOne).get();
		verify(mockFutureTwo).get();
		verify(mockFutureThree).get();
	}

	@Test
	public void testMigratePrimaryTypesUnderMaxJobs() throws InterruptedException, ExecutionException {
		// queue size larger than the number of jobs
		when(mockConfig.getMaximumNumberOfDestinationJobs()).thenReturn(destinationJobs.size()+1);
		// call under test
		asynchMigration.migratePrimaryTypes(primaryTypes);
		// should wait for all jobs to finish
		verify(mockFutureOne).get();
		verify(mockFutureTwo).get();
		verify(mockFutureThree).get();
		
		// is done is only called when queue is full
		verify(mockFutureOne, never()).isDone();
		verify(mockFutureTwo, never()).isDone();
		verify(mockFutureThree, never()).isDone();
		
		// no sleep should occur since the queue was not filled.
		verify(mockClock, never()).sleep(any(Long.class));
	}
	
	@Test
	public void testMigratePrimaryTypesOverMaxJobs() throws InterruptedException, ExecutionException {
		when(mockFutureOne.isDone()).thenReturn(false, false, true);
		when(mockFutureTwo.isDone()).thenReturn(false, false, false, true);
		when(mockFutureThree.isDone()).thenReturn(true);
		// queue size smaller than the number of jobs
		when(mockConfig.getMaximumNumberOfDestinationJobs()).thenReturn(destinationJobs.size()-1);
		// call under test
		asynchMigration.migratePrimaryTypes(primaryTypes);
		// should wait for all jobs to finish
		verify(mockFutureOne).get();
		verify(mockFutureTwo).get();
		verify(mockFutureThree).get();
		
		verify(mockFutureOne, times(3)).isDone();
		verify(mockFutureTwo, times(4)).isDone();
		verify(mockFutureThree, times(1)).isDone();
		
		// sleep each time a job is not done
		verify(mockClock, times(4)).sleep(any(Long.class));
	}
	
	@Test
	public void testMigratePrimaryTypesMultipleAsyncMigrationExceptions() throws InterruptedException, ExecutionException {
		AsyncMigrationException exceptionOne = new AsyncMigrationException("one");
		AsyncMigrationException exceptionTwo = new AsyncMigrationException("two");
		
		when(mockFutureOne.isDone()).thenReturn(false, false, true);
		// two throws on isDone()
		when(mockFutureTwo.isDone()).thenThrow(exceptionOne);
		when(mockFutureThree.isDone()).thenReturn(true);
		// threes throws on get()
		doThrow(exceptionTwo).when(mockFutureThree).get();
		// queue size smaller than the number of jobs
		when(mockConfig.getMaximumNumberOfDestinationJobs()).thenReturn(destinationJobs.size()-1);

		try {
			// call under test
			asynchMigration.migratePrimaryTypes(primaryTypes);
			fail();
		} catch (AsyncMigrationException e) {
			assertEquals(exceptionTwo, e);
		}
		// should wait for all jobs to finish
		verify(mockFutureOne).get();
		// job failed on isDone()
		verify(mockFutureTwo, never()).get();
		verify(mockFutureThree).get();
		
		verify(mockFutureOne, times(2)).isDone();
		verify(mockFutureTwo, times(1)).isDone();
		verify(mockFutureThree, times(1)).isDone();
		
		// sleep each time a job is not done
		verify(mockClock, times(2)).sleep(any(Long.class));
	}
	
	@Test
	public void testMigratePrimaryTypesGetAsyncMigrationException() throws InterruptedException, ExecutionException {
		AsyncMigrationException exceptionOne = new AsyncMigrationException("one");
		AsyncMigrationException exceptionTwo = new AsyncMigrationException("two");
		// threes throws on get()
		doThrow(exceptionOne).when(mockFutureOne).get();
		doThrow(exceptionTwo).when(mockFutureThree).get();
		// queue size smaller than the number of jobs
		when(mockConfig.getMaximumNumberOfDestinationJobs()).thenReturn(destinationJobs.size()+1);

		try {
			// call under test
			asynchMigration.migratePrimaryTypes(primaryTypes);
			fail();
		} catch (AsyncMigrationException e) {
			assertEquals(exceptionTwo, e);
		}
		// should wait for all jobs to finish
		verify(mockFutureOne).get();
		verify(mockFutureTwo).get();
		verify(mockFutureThree).get();
		
		verify(mockFutureOne, never()).isDone();
		verify(mockFutureTwo, never()).isDone();
		verify(mockFutureThree, never()).isDone();
		
		// queue is not full
		verify(mockClock, never()).sleep(any(Long.class));
	}
}
