package org.sagebionetworks.migration.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseServerException;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.repo.model.asynch.AsynchJobState;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.AsyncMigrationRequest;
import org.sagebionetworks.repo.model.migration.RestoreTypeRequest;
import org.sagebionetworks.repo.model.migration.RestoreTypeResponse;

@RunWith(MockitoJUnitRunner.class)
public class AsynchronousJobExecutorImplTest {

	@Mock
	SynapseClientFactory mockClientFactory;
	@Mock
	Configuration mockConfig;
	@Mock
	FutureFactory mockFutureFactory;
	@Mock
	SynapseAdminClient mockSourceClient;
	@Mock
	SynapseAdminClient mockDestinationClient;
	@Mock
	AsynchronousJobFuture<RestoreTypeResponse> mockSourceFuture;
	@Mock
	AsynchronousJobFuture<RestoreTypeResponse> mockDestinationFuture;

	AsynchronousJobStatus status;

	RestoreTypeRequest request;
	RestoreTypeResponse sourceReponse;
	RestoreTypeResponse destinationResponse;
	AsyncMigrationRequest migrationRequest;

	AsynchronousJobExecutorImpl jobExecutor;

	@Before
	public void before() throws SynapseException, InterruptedException, ExecutionException {

		request = new RestoreTypeRequest();
		request.setBackupFileKey("backup file");
		sourceReponse = new RestoreTypeResponse();
		sourceReponse.setRestoredRowCount(99L);
		when(mockSourceFuture.get()).thenReturn(sourceReponse);
		
		destinationResponse = new RestoreTypeResponse();
		destinationResponse.setRestoredRowCount(0L);
		when(mockDestinationFuture.get()).thenReturn(destinationResponse);


		migrationRequest = new AsyncMigrationRequest();
		migrationRequest.setAdminRequest(request);

		status = new AsynchronousJobStatus();
		status.setJobId("123");
		status.setJobState(AsynchJobState.PROCESSING);

		when(mockClientFactory.getSourceClient()).thenReturn(mockSourceClient);
		when(mockClientFactory.getDestinationClient()).thenReturn(mockDestinationClient);

		when(mockSourceClient.startAdminAsynchronousJob(migrationRequest)).thenReturn(status);
		when(mockDestinationClient.startAdminAsynchronousJob(migrationRequest)).thenReturn(status);

		when(mockFutureFactory.createFuture(status, JobTarget.SOURCE, mockSourceClient, RestoreTypeResponse.class))
				.thenReturn(mockSourceFuture);
		when(mockFutureFactory.createFuture(status, JobTarget.DESTINATION, mockDestinationClient,
				RestoreTypeResponse.class)).thenReturn(mockDestinationFuture);

		jobExecutor = new AsynchronousJobExecutorImpl(mockClientFactory, mockConfig, mockFutureFactory);
	}

	@Test
	public void testGetClientForJobTarget() {
		assertEquals(mockSourceClient, jobExecutor.getClientForJobTarget(JobTarget.SOURCE));
		assertEquals(mockDestinationClient, jobExecutor.getClientForJobTarget(JobTarget.DESTINATION));
	}

	@Test
	public void testStartJobSource() throws SynapseException {
		JobTarget jobTarget = JobTarget.SOURCE;
		// call under test
		Future<RestoreTypeResponse> future = jobExecutor.startJob(jobTarget, request, RestoreTypeResponse.class);
		assertEquals(mockSourceFuture, future);
		verify(mockSourceClient).startAdminAsynchronousJob(migrationRequest);
		// start the job on the source.
		verify(mockFutureFactory).createFuture(status, jobTarget, mockSourceClient, RestoreTypeResponse.class);
	}

	@Test
	public void testStartJobDestination() throws SynapseException {
		JobTarget jobTarget = JobTarget.DESTINATION;
		// call under test
		Future<RestoreTypeResponse> future = jobExecutor.startJob(jobTarget, request, RestoreTypeResponse.class);
		assertEquals(mockDestinationFuture, future);
		verify(mockDestinationClient).startAdminAsynchronousJob(migrationRequest);
		// start the job on the source.
		verify(mockFutureFactory).createFuture(status, jobTarget, mockDestinationClient, RestoreTypeResponse.class);
	}

	@Test(expected = AsyncMigrationException.class)
	public void StartJobSynapseException() throws SynapseException {
		SynapseServerException exception = new SynapseServerException(500);
		when(mockSourceClient.startAdminAsynchronousJob(migrationRequest)).thenThrow(exception);
		// call under test
		jobExecutor.startJob(JobTarget.SOURCE, request, RestoreTypeResponse.class);
	}

	@Test
	public void testStartDestionationJob() throws SynapseException {
		// call under test
		Future<RestoreTypeResponse> future = jobExecutor.startDestionationJob(request, RestoreTypeResponse.class);
		assertEquals(mockDestinationFuture, future);
		verify(mockDestinationClient).startAdminAsynchronousJob(migrationRequest);
		verify(mockFutureFactory).createFuture(status, JobTarget.DESTINATION, mockDestinationClient,
				RestoreTypeResponse.class);
	}

	@Test
	public void testStartSourceJob() throws SynapseException {
		// call under test
		Future<RestoreTypeResponse> future = jobExecutor.startSourceJob(request, RestoreTypeResponse.class);
		assertEquals(mockSourceFuture, future);
		verify(mockSourceClient).startAdminAsynchronousJob(migrationRequest);
		verify(mockFutureFactory).createFuture(status, JobTarget.SOURCE, mockSourceClient, RestoreTypeResponse.class);
	}

	@Test
	public void testExecuteSourceAndDestinationJob() {
		// call under test
		ResultPair<RestoreTypeResponse> results = jobExecutor.executeSourceAndDestinationJob(request,
				RestoreTypeResponse.class);
		assertNotNull(results);
		assertEquals(sourceReponse, results.getSourceResult());
		assertEquals(destinationResponse, results.getDestinationResult());
	}
	
	@Test (expected=AsyncMigrationException.class)
	public void testExecuteSourceAndDestinationJobError() throws InterruptedException, ExecutionException {
		when(mockSourceFuture.get()).thenThrow(new ExecutionException(new RuntimeException("failed")));
		// call under test
		jobExecutor.executeSourceAndDestinationJob(request,
				RestoreTypeResponse.class);
	}
	
	@Test
	public void testExecuteDestinationJob() throws AsyncMigrationException, InterruptedException {
		// call under test
		RestoreTypeResponse result = jobExecutor.executeDestinationJob(request, RestoreTypeResponse.class);
		assertEquals(destinationResponse, result);
	}
	
	@Test (expected=AsyncMigrationException.class)
	public void testExecuteDestinationJobException() throws Exception {
		when(mockDestinationFuture.get()).thenThrow(new ExecutionException(new RuntimeException("failed")));
		// call under test
		jobExecutor.executeDestinationJob(request, RestoreTypeResponse.class);
	}
	
	@Test
	public void testExecuteSourceJob() throws AsyncMigrationException, InterruptedException {
		// call under test
		RestoreTypeResponse result = jobExecutor.executeSourceJob(request, RestoreTypeResponse.class);
		assertEquals(sourceReponse, result);
	}
	
	@Test (expected=AsyncMigrationException.class)
	public void testExecuteSourceJobException() throws Exception {
		when(mockSourceFuture.get()).thenThrow(new ExecutionException(new RuntimeException("failed")));
		// call under test
		jobExecutor.executeSourceJob(request, RestoreTypeResponse.class);
	}
	
}
