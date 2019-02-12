package org.sagebionetworks.migration.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

	RestoreJobQueueImpl queue;

	RestoreDestinationJob nodeOne;
	RestoreDestinationJob nodeTwo;
	RestoreDestinationJob aclOne;
	RestoreDestinationJob aclTwo;

	@Before
	public void before() {
		when(mockLoggerFactory.getLogger(any())).thenReturn(mockLogger);
		queue = new RestoreJobQueueImpl(mockJobExecutor, mockLoggerFactory);

		nodeOne = new RestoreDestinationJob(MigrationType.NODE, "keyOne");
		nodeTwo = new RestoreDestinationJob(MigrationType.NODE, "keyTwo");
		aclOne = new RestoreDestinationJob(MigrationType.ACL, "keyThree");
		aclTwo = new RestoreDestinationJob(MigrationType.ACL, "keyFour");

		when(mockJobExecutor.startDestinationJob(nodeOne)).thenReturn(mockNodeOneFuture);
		when(mockJobExecutor.startDestinationJob(nodeTwo)).thenReturn(mockNodeTwoFuture);
		when(mockJobExecutor.startDestinationJob(aclOne)).thenReturn(mockAclOneFuture);
		when(mockJobExecutor.startDestinationJob(aclTwo)).thenReturn(mockAclTwoFuture);

		when(mockNodeOneFuture.isDone()).thenReturn(false, false, false, true);
		when(mockNodeTwoFuture.isDone()).thenReturn(false, true);
		when(mockAclOneFuture.isDone()).thenReturn(false, false, true);
		when(mockAclTwoFuture.isDone()).thenReturn(false, false, false, true);
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
	
	@Test
	public void testLastException() {
		AsyncMigrationException firstException = new AsyncMigrationException("One");
		when(mockNodeOneFuture.isDone()).thenThrow(firstException);
		AsyncMigrationException secondException = new AsyncMigrationException("two");
		when(mockNodeTwoFuture.isDone()).thenThrow(secondException);
		// push both jobs to the queue
		queue.pushJob(nodeOne);
		queue.pushJob(nodeTwo);
		
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
			assertEquals(secondException, e);
		}
		
	}
}
