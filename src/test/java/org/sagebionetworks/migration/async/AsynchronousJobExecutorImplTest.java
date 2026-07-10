package org.sagebionetworks.migration.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseBadRequestException;
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

@ExtendWith(MockitoExtension.class)
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

	@BeforeEach
	public void before() throws SynapseException, InterruptedException, ExecutionException {

		request = new RestoreTypeRequest();
		request.setBackupFileKey("backup file");
		sourceReponse = new RestoreTypeResponse();
		sourceReponse.setRestoredRowCount(99L);

		destinationResponse = new RestoreTypeResponse();
		destinationResponse.setRestoredRowCount(0L);


		migrationRequest = new AsyncMigrationRequest();
		migrationRequest.setAdminRequest(request);

		status = new AsynchronousJobStatus();
		status.setJobId("123");
		status.setJobState(AsynchJobState.PROCESSING);

		when(mockClientFactory.getSourceClient()).thenReturn(mockSourceClient);
		when(mockClientFactory.getDestinationClient()).thenReturn(mockDestinationClient);

		jobExecutor = new AsynchronousJobExecutorImpl(mockClientFactory, mockConfig, mockFutureFactory);
	}

	private void stubSuccessfulSourceJob() throws SynapseException {
		when(mockSourceClient.startAdminAsynchronousJob(migrationRequest)).thenReturn(status);
		when(mockFutureFactory.createFuture(status, JobTarget.SOURCE, mockSourceClient, RestoreTypeResponse.class))
				.thenReturn(mockSourceFuture);
	}

	private void stubSuccessfulDestinationJob() throws SynapseException {
		when(mockDestinationClient.startAdminAsynchronousJob(migrationRequest)).thenReturn(status);
		when(mockFutureFactory.createFuture(status, JobTarget.DESTINATION, mockDestinationClient,
				RestoreTypeResponse.class)).thenReturn(mockDestinationFuture);
	}

	@Test
	public void testGetClientForJobTarget() {
		assertEquals(mockSourceClient, jobExecutor.getClientForJobTarget(JobTarget.SOURCE));
		assertEquals(mockDestinationClient, jobExecutor.getClientForJobTarget(JobTarget.DESTINATION));
	}

	@Test
	public void testStartJobSource() throws SynapseException {
		stubSuccessfulSourceJob();
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
		stubSuccessfulDestinationJob();
		JobTarget jobTarget = JobTarget.DESTINATION;
		// call under test
		Future<RestoreTypeResponse> future = jobExecutor.startJob(jobTarget, request, RestoreTypeResponse.class);
		assertEquals(mockDestinationFuture, future);
		verify(mockDestinationClient).startAdminAsynchronousJob(migrationRequest);
		// start the job on the source.
		verify(mockFutureFactory).createFuture(status, jobTarget, mockDestinationClient, RestoreTypeResponse.class);
	}

	@Test
	public void StartJobSynapseException() throws SynapseException {
		SynapseServerException exception = new SynapseBadRequestException();
		when(mockSourceClient.startAdminAsynchronousJob(migrationRequest)).thenThrow(exception);
		// call under test
		assertThrows(AsyncMigrationException.class,
				() -> jobExecutor.startJob(JobTarget.SOURCE, request, RestoreTypeResponse.class));
	}

	@Test
	public void testStartDestionationJob() throws SynapseException {
		stubSuccessfulDestinationJob();
		// call under test
		Future<RestoreTypeResponse> future = jobExecutor.startDestionationJob(request, RestoreTypeResponse.class);
		assertEquals(mockDestinationFuture, future);
		verify(mockDestinationClient).startAdminAsynchronousJob(migrationRequest);
		verify(mockFutureFactory).createFuture(status, JobTarget.DESTINATION, mockDestinationClient,
				RestoreTypeResponse.class);
	}

	@Test
	public void testStartSourceJob() throws SynapseException {
		stubSuccessfulSourceJob();
		// call under test
		Future<RestoreTypeResponse> future = jobExecutor.startSourceJob(request, RestoreTypeResponse.class);
		assertEquals(mockSourceFuture, future);
		verify(mockSourceClient).startAdminAsynchronousJob(migrationRequest);
		verify(mockFutureFactory).createFuture(status, JobTarget.SOURCE, mockSourceClient, RestoreTypeResponse.class);
	}

	@Test
	public void testExecuteSourceAndDestinationJob() throws SynapseException, InterruptedException, ExecutionException {
		when(mockSourceClient.startAdminAsynchronousJob(migrationRequest)).thenReturn(status);
		when(mockDestinationClient.startAdminAsynchronousJob(migrationRequest)).thenReturn(status);
		when(mockFutureFactory.createFuture(status, JobTarget.SOURCE, mockSourceClient, RestoreTypeResponse.class))
				.thenReturn(mockSourceFuture);
		when(mockFutureFactory.createFuture(status, JobTarget.DESTINATION, mockDestinationClient,
				RestoreTypeResponse.class)).thenReturn(mockDestinationFuture);
		when(mockSourceFuture.get()).thenReturn(sourceReponse);
		when(mockDestinationFuture.get()).thenReturn(destinationResponse);
		// call under test
		ResultPair<RestoreTypeResponse> results = jobExecutor.executeSourceAndDestinationJob(request,
				RestoreTypeResponse.class);
		assertNotNull(results);
		assertEquals(sourceReponse, results.getSourceResult());
		assertEquals(destinationResponse, results.getDestinationResult());
	}

	@Test
	public void testExecuteSourceAndDestinationJobError() throws SynapseException, InterruptedException, ExecutionException {
		when(mockSourceClient.startAdminAsynchronousJob(migrationRequest)).thenReturn(status);
		when(mockDestinationClient.startAdminAsynchronousJob(migrationRequest)).thenReturn(status);
		when(mockFutureFactory.createFuture(status, JobTarget.SOURCE, mockSourceClient, RestoreTypeResponse.class))
				.thenReturn(mockSourceFuture);
		when(mockFutureFactory.createFuture(status, JobTarget.DESTINATION, mockDestinationClient,
				RestoreTypeResponse.class)).thenReturn(mockDestinationFuture);
		when(mockSourceFuture.get()).thenThrow(new ExecutionException(new RuntimeException("failed")));
		// call under test
		assertThrows(AsyncMigrationException.class,
				() -> jobExecutor.executeSourceAndDestinationJob(request, RestoreTypeResponse.class));
	}

	@Test
	public void testExecuteDestinationJob() throws SynapseException, AsyncMigrationException, InterruptedException, ExecutionException {
		stubSuccessfulDestinationJob();
		when(mockDestinationFuture.get()).thenReturn(destinationResponse);
		// call under test
		RestoreTypeResponse result = jobExecutor.executeDestinationJob(request, RestoreTypeResponse.class);
		assertEquals(destinationResponse, result);
	}

	@Test
	public void testExecuteDestinationJobException() throws SynapseException, Exception {
		when(mockDestinationClient.startAdminAsynchronousJob(migrationRequest)).thenReturn(status);
		when(mockFutureFactory.createFuture(status, JobTarget.DESTINATION, mockDestinationClient,
				RestoreTypeResponse.class)).thenReturn(mockDestinationFuture);
		when(mockDestinationFuture.get()).thenThrow(new ExecutionException(new RuntimeException("failed")));
		// call under test
		assertThrows(AsyncMigrationException.class,
				() -> jobExecutor.executeDestinationJob(request, RestoreTypeResponse.class));
	}

	@Test
	public void testExecuteSourceJob() throws SynapseException, AsyncMigrationException, InterruptedException, ExecutionException {
		stubSuccessfulSourceJob();
		when(mockSourceFuture.get()).thenReturn(sourceReponse);
		// call under test
		RestoreTypeResponse result = jobExecutor.executeSourceJob(request, RestoreTypeResponse.class);
		assertEquals(sourceReponse, result);
	}

	@Test
	public void testExecuteSourceJobException() throws SynapseException, Exception {
		when(mockSourceClient.startAdminAsynchronousJob(migrationRequest)).thenReturn(status);
		when(mockFutureFactory.createFuture(status, JobTarget.SOURCE, mockSourceClient, RestoreTypeResponse.class))
				.thenReturn(mockSourceFuture);
		when(mockSourceFuture.get()).thenThrow(new ExecutionException(new RuntimeException("failed")));
		// call under test
		assertThrows(AsyncMigrationException.class,
				() -> jobExecutor.executeSourceJob(request, RestoreTypeResponse.class));
	}

}
