package org.sagebionetworks.migration.async;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.mockito.Mock;

import org.mockito.MockitoAnnotations;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.repo.model.asynch.AsynchJobState;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.*;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class AsyncMigrationTypeCountsWorkerTest {

	@Mock
	private SynapseAdminClient mockClient;


	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}

	@After
	public void tearDown() {

	}

	@Test
	public void testOK() throws Exception {
		// Some types
		List<MigrationType> types = new LinkedList<MigrationType>();
		types.add(MigrationType.ACCESS_APPROVAL);
		// Expected from mockClient
		AsynchronousJobStatus expectedStatus = new AsynchronousJobStatus();
		expectedStatus.setJobId("1");
		expectedStatus.setJobState(AsynchJobState.COMPLETE);
		MigrationTypeCounts expectedTypeCounts = new MigrationTypeCounts();
		List<MigrationTypeCount> l = new LinkedList<MigrationTypeCount>();
		MigrationTypeCount tc1 = new MigrationTypeCount();
		tc1.setType(MigrationType.ACCESS_APPROVAL);
		tc1.setCount(10L);
		l.add(tc1);
		expectedTypeCounts.setList(l);
		AsyncMigrationResponse expectedResponse = new AsyncMigrationResponse();
		expectedResponse.setAdminResponse(expectedTypeCounts);
		expectedStatus.setResponseBody(expectedResponse);

		when(mockClient.startAdminAsynchronousJob(any(AsyncMigrationRequest.class))).thenReturn(expectedStatus);
		when(mockClient.getAdminAsynchronousJobStatus("1")).thenReturn(expectedStatus);

		AsyncMigrationTypeCountsWorker worker = new AsyncMigrationTypeCountsWorker(mockClient, types, 1000);

		// Call under test
		MigrationTypeCounts typeCounts = worker.call();

		assertNotNull(typeCounts);
		assertNotNull(typeCounts.getList());
		assertEquals(1, typeCounts.getList().size());
		assertEquals(MigrationType.ACCESS_APPROVAL, typeCounts.getList().get(0).getType());

	}

	@Test(expected = AsyncMigrationException.class)
	public void testException() throws Exception {
		// Some types
		List<MigrationType> types = new LinkedList<MigrationType>();
		types.add(MigrationType.ACCESS_APPROVAL);
		// Expected exception
		TimeoutException timeoutException = new TimeoutException("Timed out waiting for the job to complete");
		AsyncMigrationException expectedException = new AsyncMigrationException(timeoutException);
		// Technically it would not throw that exception on start...
		when(mockClient.startAdminAsynchronousJob(any(AsyncMigrationRequest.class))).thenThrow(expectedException);

		AsyncMigrationTypeCountsWorker worker = new AsyncMigrationTypeCountsWorker(mockClient, types, 1000);

		// Call under test
		MigrationTypeCounts typeCounts = worker.call();
	}

}