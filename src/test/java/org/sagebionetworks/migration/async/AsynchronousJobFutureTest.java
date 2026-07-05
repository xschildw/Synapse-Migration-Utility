package org.sagebionetworks.migration.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.sagebionetworks.migration.async.AsynchronousJobFuture.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseBadRequestException;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseServerException;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.Reporter;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.repo.model.asynch.AsynchJobState;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.AsyncMigrationResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.RestoreTypeResponse;
import org.sagebionetworks.util.Clock;

@RunWith(MockitoJUnitRunner.class)
public class AsynchronousJobFutureTest {

	@Mock
	SynapseAdminClient mockClient;
	@Mock
	Reporter mockReporter;
	@Mock
	Clock mockClock;
	@Mock
	Configuration mockConfiguration;
	
	String jobId;
	AsynchronousJobStatus processingStatus;
	AsynchronousJobStatus completeStatus;
	AsynchronousJobStatus failedStatus;
	
	AsyncMigrationResponse jobResponse;
	RestoreTypeResponse wrappedReponse;
	String errorMessage;
	
	JobTarget jobTarget;
	
	long defaultTimeoutMS;
	
	FutureFactoryImpl futureFactory;
	AsynchronousJobFuture<RestoreTypeResponse> future;
	
	MigrationType type;
	
	@Before
	public void before() throws SynapseException {
		jobId = "123";
		jobTarget = JobTarget.DESTINATION;
		
		wrappedReponse = new RestoreTypeResponse();
		wrappedReponse.setRestoredRowCount(99L);
		
		jobResponse = new AsyncMigrationResponse();
		jobResponse.setAdminResponse(wrappedReponse);
		
		processingStatus = new AsynchronousJobStatus();
		processingStatus.setJobId(jobId);
		processingStatus.setJobState(AsynchJobState.PROCESSING);
		
		completeStatus = new AsynchronousJobStatus();
		completeStatus.setJobId(jobId);
		completeStatus.setJobState(AsynchJobState.COMPLETE);
		completeStatus.setResponseBody(jobResponse);
		
		errorMessage = "some kind of error";
		failedStatus = new AsynchronousJobStatus();
		failedStatus.setJobId(jobId);
		failedStatus.setJobState(AsynchJobState.FAILED);
		failedStatus.setErrorMessage(errorMessage);
		
		defaultTimeoutMS = 11;
		when(mockClock.currentTimeMillis()).thenReturn(5000L,5001L,5002L,5003L,5004L,5005L,5006L,5007L,5008L,5009L);
		// complete after two tries
		when(mockClient.getAdminAsynchronousJobStatus(jobId)).thenReturn(processingStatus, processingStatus, completeStatus);
		type = MigrationType.NODE;
		// Using the factory to create the future also tests the factory.
		futureFactory = new FutureFactoryImpl(mockReporter, mockClock, mockConfiguration);
		future = (AsynchronousJobFuture<RestoreTypeResponse>) futureFactory.createFuture(processingStatus, jobTarget,  mockClient, RestoreTypeResponse.class);
		

	}
	
	@Test
	public void testIsDoneComlete() throws SynapseException {
		// complete after two tries
		when(mockClient.getAdminAsynchronousJobStatus(jobId)).thenReturn(processingStatus, processingStatus, completeStatus);
		assertFalse(future.isDone());
		assertFalse(future.isDone());
		// Complete is done
		assertTrue(future.isDone());
		assertTrue(future.isDone());
		// once done no more get status calls should occur
		verify(mockClient, times(3)).getAdminAsynchronousJobStatus(jobId);
		verify(mockReporter, times(1)).reportProgress(jobTarget, processingStatus);
	}
	
	
	@Test
	public void testIsDoneFailed() throws SynapseException {
		// failed after two tries
		when(mockClient.getAdminAsynchronousJobStatus(jobId)).thenReturn(processingStatus, processingStatus, failedStatus);
		assertFalse(future.isDone());
		assertFalse(future.isDone());
		// failed is done
		assertTrue(future.isDone());
		assertTrue(future.isDone());
		// once done no more get status calls should occur
		verify(mockClient, times(3)).getAdminAsynchronousJobStatus(jobId);
		verify(mockReporter, times(1)).reportProgress(jobTarget, processingStatus);
	}
	
	@Test
	public void testIsDoneReportThrottled() throws SynapseException {
		// setup the clock such that every other call should trigger report.
		Long startTime = MINIMUM_MS_BETWEEN_REPORTS+1;
		Long halfTime = MINIMUM_MS_BETWEEN_REPORTS/2L;
		when(mockClock.currentTimeMillis()).thenReturn(
				startTime
				,startTime+(1*halfTime)
				,startTime+(2*halfTime)
				,startTime+(3*halfTime)
				,startTime+(4*halfTime)
		);

		// complete after two tries
		when(mockClient.getAdminAsynchronousJobStatus(jobId)).thenReturn(processingStatus, processingStatus, processingStatus, completeStatus);
		// calls under test
		assertFalse(future.isDone());
		assertFalse(future.isDone());
		assertFalse(future.isDone());
		assertTrue(future.isDone());
		// progress should occur the first time, then every other time.
		verify(mockReporter, times(2)).reportProgress(jobTarget, processingStatus);
	}
	
	@Test (expected=AsyncMigrationException.class)
	public void testIsDoneException() throws SynapseException {
		SynapseServerException error = new SynapseBadRequestException();
		// failed after two tries
		when(mockClient.getAdminAsynchronousJobStatus(jobId)).thenThrow(error);
		// call under test;
		future.isDone();
	}
	
	/**
	 * Get with timeout and units.
	 * @throws Exception
	 */
	@Test
	public void testGetTimeoutUnits() throws Exception {
		long timeout = 1;
		TimeUnit unit = TimeUnit.HOURS;
		// call under test
		RestoreTypeResponse result = future.get(timeout, unit);
		assertEquals(wrappedReponse, result);
		// should sleep twice.
		verify(mockClock,times(2)).sleep(AsynchronousJobFuture.SLEEP_TIME);
		verify(mockClock, atLeast(3)).currentTimeMillis();
		verify(mockReporter, times(1)).reportProgress(jobTarget, processingStatus);
	}
	
	@Test
	public void testGetTimeoutUnitsExpired() throws Exception {
		long timeout = 1;
		TimeUnit unit = TimeUnit.MILLISECONDS;
		try {
			// call under test
			future.get(timeout, unit);
			fail();
		} catch (TimeoutException e) {
			// expected
			assertEquals(AsynchronousJobFuture.TIMEOUT_MESSAGE, e.getMessage());
		}
		verify(mockClock, times(3)).currentTimeMillis();
		verify(mockReporter, times(1)).reportProgress(jobTarget, processingStatus);
	}
	
	@Test
	public void testGetTimeoutUnitsJobFailed() throws Exception {
		// job failed
		when(mockClient.getAdminAsynchronousJobStatus(jobId)).thenReturn(processingStatus, processingStatus, failedStatus);
		long timeout = 1;
		TimeUnit unit = TimeUnit.DAYS;
		try {
			// call under test
			future.get(timeout, unit);
			fail();
		} catch (AsyncMigrationException e) {
			// expected
			assertTrue(e.getMessage().contains(errorMessage));
		}
	}
	
	@Test
	public void testGet() throws InterruptedException, ExecutionException {
		// timeout from the config.
		when(mockConfiguration.getWorkerTimeoutMs()).thenReturn(100L);
		// create a new future that uses this timeout.
		future = (AsynchronousJobFuture<RestoreTypeResponse>) futureFactory.createFuture(processingStatus, jobTarget, mockClient, RestoreTypeResponse.class);
		// call under test
		RestoreTypeResponse result = future.get();
		assertEquals(wrappedReponse, result);
	}
	
	@Test
	public void testGetTimeout() throws InterruptedException, ExecutionException {
		// timeout from the config.
		when(mockConfiguration.getWorkerTimeoutMs()).thenReturn(1L);
		// create a new future that uses this timeout.
		future = (AsynchronousJobFuture<RestoreTypeResponse>) futureFactory.createFuture(processingStatus, jobTarget, mockClient, RestoreTypeResponse.class);
		// call under test
		try {
			future.get();
			fail();
		} catch (AsyncMigrationException e) {
			// For this case the timeout is wrapped in AsyncMigrationException.
			assertTrue(e.getCause() instanceof TimeoutException);
		}
	}
}
