package org.sagebionetworks.migration.async;

import org.junit.Before;
import org.junit.Test;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;


import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseServerException;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.repo.model.asynch.AsynchJobState;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.*;

import java.util.concurrent.TimeoutException;

public class AsyncMigrationIdRangeChecksumWorkerTest {

	@Mock
	SynapseAdminClient mockClient;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void testSyncOK() throws Exception {
		MigrationRangeChecksum expectedChecksum = new MigrationRangeChecksum();
		expectedChecksum.setType(MigrationType.ACL);
		expectedChecksum.setMinid(0L);
		expectedChecksum.setMaxid(100L);
		expectedChecksum.setChecksum("checksum");

		when(mockClient.getChecksumForIdRange(MigrationType.ACL, "salt", 0L, 100L)).thenReturn(expectedChecksum);

		AsyncMigrationIdRangeChecksumWorker worker = new AsyncMigrationIdRangeChecksumWorker(mockClient, MigrationType.ACL, "salt", 0L, 100L, 1000L);

		// Call under test
		MigrationRangeChecksum checksum = worker.call();

		assertNotNull(checksum);
		assertEquals(expectedChecksum, checksum);
		verify(mockClient, never()).startAdminAsynchronousJob(any(AsyncMigrationRequest.class));
		verify(mockClient, never()).getAsynchronousJobStatus(anyString());

	}

	@Test
	public void testAsyncOK() throws Exception {
		// Eventual result
		MigrationRangeChecksum expectedChecksum = new MigrationRangeChecksum();
		expectedChecksum.setType(MigrationType.ACL);
		expectedChecksum.setMinid(0L);
		expectedChecksum.setMaxid(100L);
		expectedChecksum.setChecksum("checksum");

		// Expected from mockClient on startJob, getJobStatus
		AsynchronousJobStatus expectedStatus = new AsynchronousJobStatus();
		expectedStatus.setJobId("1");
		expectedStatus.setJobState(AsynchJobState.COMPLETE);
		AsyncMigrationResponse expectedResponse = new AsyncMigrationResponse();
		expectedResponse.setAdminResponse(expectedChecksum);
		expectedStatus.setResponseBody(expectedResponse);

		SynapseException expectedException = new SynapseServerException(500);
		when(mockClient.getChecksumForIdRange(MigrationType.ACL, "salt", 0L, 100L)).thenThrow(expectedException);
		when(mockClient.startAdminAsynchronousJob(any(AsyncMigrationRequest.class))).thenReturn(expectedStatus);
		when(mockClient.getAdminAsynchronousJobStatus("1")).thenReturn(expectedStatus);

		AsyncMigrationIdRangeChecksumWorker worker = new AsyncMigrationIdRangeChecksumWorker(mockClient, MigrationType.ACL, "salt", 0L, 100L, 1000L);

		// Call under test
		MigrationRangeChecksum checksum = worker.call();

		assertNotNull(checksum);
		assertEquals(expectedChecksum, checksum);

		verify(mockClient).getChecksumForIdRange(MigrationType.ACL, "salt", 0L, 100L);
		verify(mockClient).startAdminAsynchronousJob(any(AsyncMigrationRequest.class));
		verify(mockClient).getAdminAsynchronousJobStatus(anyString());

	}

	@Test(expected = AsyncMigrationException.class)
	public void testException() throws Exception {
		// Expected from mockClient on startJob, getJobStatus
		AsynchronousJobStatus expectedStatus = new AsynchronousJobStatus();
		expectedStatus.setJobId("1");
		expectedStatus.setJobState(AsynchJobState.FAILED);
		expectedStatus.setErrorMessage("Exception in async migration job!");
		expectedStatus.setErrorDetails("Failed because of some error");
		AsyncMigrationResponse expectedResponse = new AsyncMigrationResponse();
		expectedResponse.setAdminResponse(null);
		expectedStatus.setResponseBody(expectedResponse);

		SynapseException expectedException = new SynapseServerException(500);
		when(mockClient.getChecksumForIdRange(MigrationType.ACL, "salt", 0L, 100L)).thenThrow(expectedException);
		when(mockClient.startAdminAsynchronousJob(any(AsyncMigrationRequest.class))).thenReturn(expectedStatus);
		when(mockClient.getAdminAsynchronousJobStatus("1")).thenReturn(expectedStatus);

		AsyncMigrationIdRangeChecksumWorker worker = new AsyncMigrationIdRangeChecksumWorker(mockClient, MigrationType.ACL, "salt", 0L, 100L, 1000L);

		// Call under test
		MigrationRangeChecksum checksum = worker.call();

	}
}