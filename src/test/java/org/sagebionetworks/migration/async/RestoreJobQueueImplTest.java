package org.sagebionetworks.migration.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.LoggerFactory;
import org.sagebionetworks.repo.model.migration.MigrationType;

@RunWith(MockitoJUnitRunner.class)
public class RestoreJobQueueImplTest {

	@Mock
	DestinationJobExecutor mockJobExecutor;
	@Mock
	LoggerFactory mockLoggerFactory;
	@Mock
	Logger mockLogger;
	@Mock
	Future mockNodeOneFuture;
	@Mock
	Future mockNodeTwoFuture;
	@Mock
	Future mockAclOneFuture;
	@Mock
	Future mockAclTwoFuture;
	@Mock
	Future mockChangeFuture;

	RestoreJobQueueImpl queue;

	RestoreDestinationJob nodeOne;
	RestoreDestinationJob nodeTwo;
	RestoreDestinationJob aclOne;
	RestoreDestinationJob aclTwo;
	RestoreDestinationJob changeJob;
	

	@Before
	public void before() {
		when(mockLoggerFactory.getLogger(any())).thenReturn(mockLogger);
		queue = new RestoreJobQueueImpl(mockJobExecutor, mockLoggerFactory);

		nodeOne = new RestoreDestinationJob(MigrationType.NODE, "keyOne");
		nodeTwo = new RestoreDestinationJob(MigrationType.NODE, "keyTwo");
		aclOne = new RestoreDestinationJob(MigrationType.ACL, "keyThree");
		aclTwo = new RestoreDestinationJob(MigrationType.ACL, "keyFour");
		changeJob = new RestoreDestinationJob(MigrationType.CHANGE, "keyChange");

		when(mockJobExecutor.startDestinationJob(nodeOne)).thenReturn(mockNodeOneFuture);
		when(mockJobExecutor.startDestinationJob(nodeTwo)).thenReturn(mockNodeTwoFuture);
		when(mockJobExecutor.startDestinationJob(aclOne)).thenReturn(mockAclOneFuture);
		when(mockJobExecutor.startDestinationJob(aclTwo)).thenReturn(mockAclTwoFuture);
		when(mockJobExecutor.startDestinationJob(changeJob)).thenReturn(mockChangeFuture);

		when(mockNodeOneFuture.isDone()).thenReturn(false, false, false, true);
		when(mockNodeTwoFuture.isDone()).thenReturn(false, true);
		when(mockAclOneFuture.isDone()).thenReturn(false, false, true);
		when(mockAclTwoFuture.isDone()).thenReturn(false, false, false, true);
		
		when(mockChangeFuture.isDone()).thenReturn(false, true);
	}

	@Test
	public void testAll() {
		// add all of the jobs to the queue
		queue.pushJob(nodeOne);
		queue.pushJob(nodeTwo);
		queue.pushJob(aclOne);
		queue.pushJob(aclTwo);

		// Fire the timer until all jobs are done
		while (!queue.isDone()) {
			queue.timerFired();
		}

		// all four jobs should be started
		verify(mockJobExecutor, times(4)).startDestinationJob(any(DestinationJob.class));
		verify(mockJobExecutor).startDestinationJob(nodeOne);
		verify(mockJobExecutor).startDestinationJob(nodeTwo);
		verify(mockJobExecutor).startDestinationJob(aclOne);
		verify(mockJobExecutor).startDestinationJob(aclTwo);

		verify(mockLogger, times(3)).info("Currently running: 2 restore jobs.  Waiting to start 2 restore jobs.");
		verify(mockLogger, times(1)).info("Currently running: 2 restore jobs.  Waiting to start 1 restore jobs.");
		verify(mockLogger, times(2)).info("Currently running: 2 restore jobs.  Waiting to start 0 restore jobs.");
		verify(mockLogger, times(1)).info("Currently running: 1 restore jobs.  Waiting to start 0 restore jobs.");
	}
	
	/**
	 * Change jobs cannot be run at the same time as any other jobs.
	 */
	@Test
	public void testChagneJobs() {
		// add all of the jobs to the queue
		queue.pushJob(nodeOne);
		queue.pushJob(aclOne);
		queue.pushJob(changeJob);

		// Fire the timer until all jobs are done
		while (!queue.isDone()) {
			queue.timerFired();
		}
		
		verify(mockJobExecutor, times(3)).startDestinationJob(any(DestinationJob.class));
		verify(mockJobExecutor).startDestinationJob(nodeOne);
		verify(mockJobExecutor).startDestinationJob(aclOne);
		verify(mockJobExecutor).startDestinationJob(changeJob);

		verify(mockLogger, times(3)).info("Currently running: 2 restore jobs.  Waiting to start 1 restore jobs.");
		verify(mockLogger, times(1)).info("Currently running: 1 restore jobs.  Waiting to start 1 restore jobs.");
		verify(mockLogger, times(2)).info("Currently running: 1 restore jobs.  Waiting to start 0 restore jobs.");
	}
	
	@Test
	public void testLastException() throws InterruptedException, ExecutionException {
		when(mockNodeOneFuture.isDone()).thenReturn(true);
		AsyncMigrationException firstException = new AsyncMigrationException("One");
		when(mockNodeOneFuture.get()).thenThrow(firstException);
		when(mockNodeTwoFuture.isDone()).thenReturn(true);
		AsyncMigrationException secondException = new AsyncMigrationException("two");
		when(mockNodeTwoFuture.get()).thenThrow(secondException);
		// push both jobs to the queue
		queue.pushJob(nodeOne);
		queue.pushJob(nodeTwo);
		
		assertFalse(queue.isDone());
		queue.timerFired();
		assertFalse(queue.isDone());
		queue.timerFired();
		assertFalse(queue.isDone());
		queue.timerFired();
		assertFalse(queue.isDone());
		queue.timerFired();
		try {
			// second exception should be thrown.
			queue.isDone();
			fail();
		}catch(AsyncMigrationException e) {
			assertEquals(secondException, e.getCause());
		}
	}
	
	/**
	 * See: PLFM-5474
	 * An unexpected exception should terminate
	 * at isDone().
	 */
	@Test
	public void testForPLFM_5474IsDone(){
		OutOfMemoryError terminate = new OutOfMemoryError("Out of memory");
		when(mockJobExecutor.startDestinationJob(nodeOne)).thenThrow(terminate);
		// push one job
		queue.pushJob(nodeOne);
		// Exception should not be thrown on timer fired
		queue.timerFired();
		try {
			queue.isDone();
			fail();
		}catch(RuntimeException e) {
			assertEquals(e.getCause(), terminate);
		}
	}
	
	/**
	 * See: PLFM-5474
	 * An unexpected exception should terminate
	 * at isDone().
	 */
	@Test
	public void testForPLFM_5474PushJob(){
		OutOfMemoryError terminate = new OutOfMemoryError("Out of memory");
		when(mockJobExecutor.startDestinationJob(nodeOne)).thenThrow(terminate);
		// push one job
		queue.pushJob(nodeOne);
		// Exception should not be thrown on timer fired
		queue.timerFired();
		try {
			// call under test
			queue.pushJob(nodeTwo);
			fail();
		}catch(RuntimeException e) {
			assertEquals(e.getCause(), terminate);
		}
	}
	
	@Test
	public void testForPLFM_5474StartAsych(){
		AsyncMigrationException nonTermiante = new AsyncMigrationException("Some random exception");
		// fail the first time then succeed the second time.
		when(mockJobExecutor.startDestinationJob(nodeOne)).thenThrow(nonTermiante).thenReturn(mockNodeOneFuture);
		when(mockNodeOneFuture.isDone()).thenReturn(true);
		// push one job
		queue.pushJob(nodeOne);
		// First will fail
		queue.timerFired();
		assertFalse(queue.isDone());
		// Second works
		queue.timerFired();
		assertFalse(queue.isDone());
		// second should finish
		queue.timerFired();
		try {
			// Exception from first failure should be exposed.
			queue.isDone();
			fail();
		}catch(RuntimeException e) {
			assertEquals(e.getCause(), nonTermiante);
		}
	}
	
}
