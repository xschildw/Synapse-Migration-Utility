package org.sagebionetworks.migration.utils;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.sagebionetworks.migration.AsyncMigrationWorker;
import org.sagebionetworks.migration.WorkerFailedException;
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class ConcurrentAdminRequestExecutorTest {

	ConcurrentAdminRequestExecutor executor;
	ExecutorService threadPool;

	@Before
	public void setUp() throws Exception {
		threadPool = Executors.newFixedThreadPool(2);
		executor = new ConcurrentAdminRequestExecutor(threadPool);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void executeRequestsOK() throws Exception {
		MigrationTypeCount mtc = new MigrationTypeCount();
		mtc.setType(MigrationType.ACL);
		mtc.setMinid(0L);
		mtc.setMaxid(10L);
		mtc.setCount(10L);
		AsyncMigrationWorker mockWorker1 = Mockito.mock(AsyncMigrationWorker.class);
		when(mockWorker1.call()).thenReturn(mtc);
		MigrationTypeChecksum mtcks = new MigrationTypeChecksum();
		mtcks.setType(MigrationType.ACL);
		mtcks.setChecksum("checksum");
		AsyncMigrationWorker mockWorker2 = Mockito.mock(AsyncMigrationWorker.class);
		when(mockWorker2.call()).thenReturn(mtcks);
		List<AsyncMigrationWorker> workers = Arrays.asList(mockWorker1, mockWorker2);
		List<AdminResponse> expectedResponses = new LinkedList<AdminResponse>();
		expectedResponses.add(mtc);
		expectedResponses.add(mtcks);

		// Call under test
		List<AdminResponse> responses = executor.executeRequests(workers);

		assertEquals(2, responses.size());
		assertEquals(expectedResponses, responses);
	}

	@Test
	public void executeRequestError() throws Exception {
		MigrationTypeCount mtc = new MigrationTypeCount();
		mtc.setType(MigrationType.ACL);
		mtc.setMinid(0L);
		mtc.setMaxid(10L);
		mtc.setCount(10L);
		AsyncMigrationWorker mockWorker1 = Mockito.mock(AsyncMigrationWorker.class);
		when(mockWorker1.call()).thenReturn(mtc);
		MigrationTypeChecksum mtcks = new MigrationTypeChecksum();
		mtcks.setType(MigrationType.ACL);
		mtcks.setChecksum("checksum");
		AsyncMigrationWorker mockWorker2 = Mockito.mock(AsyncMigrationWorker.class);
		WorkerFailedException expectedException = new WorkerFailedException("Illegal Argument in MigrationTypeChecksumRequest!");
		when(mockWorker2.call()).thenThrow(expectedException);
		List<AsyncMigrationWorker> workers = Arrays.asList(mockWorker1, mockWorker2);
		List<AdminResponse> expectedResponses = new LinkedList<AdminResponse>();
		expectedResponses.add(mtc);
		expectedResponses.add(null);

		// Call under test
		List<AdminResponse> responses = executor.executeRequests(workers);

		assertEquals(2, responses.size());
		assertEquals(expectedResponses, responses);
	}

	@Test
	public void executeRequestTimeout() throws Exception {
		MigrationTypeCount mtc = new MigrationTypeCount();
		mtc.setType(MigrationType.ACL);
		mtc.setMinid(0L);
		mtc.setMaxid(10L);
		mtc.setCount(10L);
		AsyncMigrationWorker mockWorker1 = Mockito.mock(AsyncMigrationWorker.class);
		when(mockWorker1.call()).thenReturn(mtc);
		MigrationTypeChecksum mtcks = new MigrationTypeChecksum();
		mtcks.setType(MigrationType.ACL);
		mtcks.setChecksum("checksum");
		AsyncMigrationWorker mockWorker2 = Mockito.mock(AsyncMigrationWorker.class);
		InterruptedException expectedException = new InterruptedException("Timeout in MigrationTypeChecksumRequest!");
		when(mockWorker2.call()).thenThrow(expectedException);
		List<AsyncMigrationWorker> workers = Arrays.asList(mockWorker1, mockWorker2);
		List<AdminResponse> expectedResponses = new LinkedList<AdminResponse>();
		expectedResponses.add(mtc);
		expectedResponses.add(null);

		// Call under test
		List<AdminResponse> responses = executor.executeRequests(workers);

		assertEquals(2, responses.size());
		assertEquals(expectedResponses, responses);
	}
}