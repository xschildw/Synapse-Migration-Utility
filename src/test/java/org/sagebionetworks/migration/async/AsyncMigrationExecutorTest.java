package org.sagebionetworks.migration.async;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;

import org.mockito.internal.util.reflection.Whitebox;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseServerException;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.WorkerFailedException;
import org.sagebionetworks.repo.model.asynch.AsynchJobState;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.AdminRequest;
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.repo.model.migration.AsyncMigrationRequest;
import org.sagebionetworks.repo.model.migration.AsyncMigrationResponse;
import org.sagebionetworks.repo.model.status.StatusEnum;
import org.sagebionetworks.util.Clock;

import java.util.concurrent.TimeoutException;

public class AsyncMigrationExecutorTest {
	
	@Mock
	private SynapseAdminClient mockClient;
	@Mock
	private Clock mockClock;
	@Mock
	private AdminRequest request;
	@Mock
	private AdminResponse response;
	
	private AsyncMigrationRequestExecutor worker;
	private AsyncMigrationRequest migReq;
	private AsyncMigrationResponse migResp;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		AsynchronousJobStatus expectedStartStatus = new AsynchronousJobStatus();
		expectedStartStatus.setJobId("jobId");
		expectedStartStatus.setJobState(AsynchJobState.PROCESSING);
		migReq = new AsyncMigrationRequest();
		migReq.setAdminRequest(request);
		migResp = new AsyncMigrationResponse();
		migResp.setAdminResponse(response);
		when(mockClient.startAdminAsynchronousJob(migReq)).thenReturn(expectedStartStatus);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testTimeout() throws Exception {
		when(mockClock.currentTimeMillis()).thenReturn(1000L, 2500L);

		worker = new AsyncMigrationRequestExecutor(mockClient, request, 1000L);
		worker.setClock(mockClock);
		
		// Call under test
		try {
			AdminResponse resp = worker.execute();
		} catch (AsyncMigrationException e) {
			assertTrue(e.getCause() instanceof TimeoutException);
			assertEquals(e.getMessage(), e.getCause().getMessage());
		}

		verify(mockClient, never()).getAsynchronousJobStatus(anyString());
		verify(mockClock, never()).sleep(anyLong());
	}
	
	@Test
	public void testFailed() throws Exception {
		when(mockClock.currentTimeMillis()).thenReturn(1000L, 2500L);
		AsynchronousJobStatus jobStatus1 = new AsynchronousJobStatus();
		jobStatus1.setJobId("jobId");
		jobStatus1.setJobState(AsynchJobState.FAILED);
		jobStatus1.setErrorDetails("Error details");
		when(mockClient.getAdminAsynchronousJobStatus("jobId")).thenReturn(jobStatus1);
		
		worker = new AsyncMigrationRequestExecutor(mockClient, request, 10000L);
		worker.setClock(mockClock);
		
		// Call under test
		try {
			AdminResponse resp = worker.execute();
		} catch (AsyncMigrationException e) {
			assertTrue(e.getCause() instanceof WorkerFailedException);
			assertEquals(e.getMessage(), e.getCause().getMessage());
		}

		verify(mockClock, never()).sleep(anyLong());
	}

	@Test
	public void testSynapseException() throws Exception {
		when(mockClock.currentTimeMillis()).thenReturn(1000L, 2500L);
		SynapseException expectedException = new SynapseServerException(500, "msg");
		when(mockClient.getAdminAsynchronousJobStatus(anyString())).thenThrow(expectedException);

		worker = new AsyncMigrationRequestExecutor(mockClient, request, 10000L);
		worker.setClock(mockClock);

		// Call under test
		try {
			AdminResponse resp = worker.execute();
		} catch (AsyncMigrationException e) {
			assertTrue(e.getCause() instanceof SynapseServerException);
			assertTrue(e.getCause().getMessage().contains("Status Code: 500"));
		}

		verify(mockClock, times(2)).sleep(anyLong());
	}

	@Test
	public void testSynapseExceptionMaxRetry() throws Exception {
		when(mockClock.currentTimeMillis()).thenReturn(1000L, 2500L);
		SynapseException expectedException = new SynapseServerException(500);
		when(mockClient.getAdminAsynchronousJobStatus(anyString())).thenThrow(expectedException);

		worker = new AsyncMigrationRequestExecutor(mockClient, request, 10000L);
		worker.setClock(mockClock);

		// Call under test
		try {
			AdminResponse resp = worker.execute();
		} catch (AsyncMigrationException e) {
			assertTrue(e.getCause() instanceof SynapseServerException);
			assertEquals("AsyncMigrationRequestExecutor: number of retries exceeded.", e.getMessage());
			assertTrue(e.getCause().getMessage().contains("Status Code: 500"));
		}

		verify(mockClock, times(2)).sleep(anyLong());
	}

	@Test
	public void testSynapseExceptionRetry() throws Exception {
		when(mockClock.currentTimeMillis()).thenReturn(1000L, 2500L);
		SynapseException expectedException = new SynapseServerException(500);
		AsynchronousJobStatus expectedJobStatus = new AsynchronousJobStatus();
		expectedJobStatus.setJobState(AsynchJobState.COMPLETE);
		expectedJobStatus.setJobId("1");
		expectedJobStatus.setResponseBody(new AsyncMigrationResponse());
		when(mockClient.getAdminAsynchronousJobStatus(anyString())).thenThrow(expectedException).thenReturn(expectedJobStatus);

		worker = new AsyncMigrationRequestExecutor(mockClient, request, 10000L);
		worker.setClock(mockClock);

		AdminResponse resp = worker.execute();

		verify(mockClock, times(1)).sleep(anyLong());
	}

	@Test
	public void testComplete() throws Exception {
		when(mockClock.currentTimeMillis()).thenReturn(1000L, 2000L);
		AsynchronousJobStatus jobStatus1 = new AsynchronousJobStatus();
		jobStatus1.setJobId("jobId");
		jobStatus1.setProgressCurrent(100L);
		jobStatus1.setProgressTotal(100L);
		jobStatus1.setProgressMessage("Completed...");
		jobStatus1.setJobState(AsynchJobState.COMPLETE);
		jobStatus1.setResponseBody(migResp);
		when(mockClient.getAdminAsynchronousJobStatus("jobId")).thenReturn(jobStatus1);

		worker = new AsyncMigrationRequestExecutor(mockClient, request, 3000L);
		worker.setClock(mockClock);
		
		// Call under test
		AdminResponse resp = worker.execute();
		
		assertNotNull(resp);

		verify(mockClock, never()).sleep(anyLong());
	}
	
	@Test
	public void testProcessing() throws Exception {
		when(mockClock.currentTimeMillis()).thenReturn(1000L, 2000L, 3000L, 4000L);
		AsynchronousJobStatus jobStatus1 = new AsynchronousJobStatus();
		jobStatus1.setJobId("jobId");
		jobStatus1.setProgressCurrent(50L);
		jobStatus1.setProgressTotal(100L);
		jobStatus1.setProgressMessage("Progressing...");
		jobStatus1.setJobState(AsynchJobState.PROCESSING);
		AsynchronousJobStatus jobStatus2 = new AsynchronousJobStatus();
		jobStatus2.setJobId("jobId");
		jobStatus2.setProgressCurrent(100L);
		jobStatus2.setProgressTotal(100L);
		jobStatus2.setJobState(AsynchJobState.COMPLETE);
		jobStatus2.setResponseBody(migResp);
		when(mockClient.getAdminAsynchronousJobStatus("jobId")).thenReturn(jobStatus1, jobStatus2);
		
		worker = new AsyncMigrationRequestExecutor(mockClient, request, 3000L);
		worker.setClock(mockClock);
		
		// Call under test
		AdminResponse resp = worker.execute();
		
		assertNotNull(resp);
		
		verify(mockClock, times(1)).sleep(1000L);
	}
	
}
