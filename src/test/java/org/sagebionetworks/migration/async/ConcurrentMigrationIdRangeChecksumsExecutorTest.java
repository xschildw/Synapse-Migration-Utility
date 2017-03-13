package org.sagebionetworks.migration.async;

import com.amazonaws.services.simpleworkflow.model.ContinueAsNewWorkflowExecutionFailedCause;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.factory.AsyncMigrationIdRangeChecksumWorkerFactory;
import org.sagebionetworks.repo.model.migration.MigrationRangeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationType;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class ConcurrentMigrationIdRangeChecksumsExecutorTest {
	@Mock
	private SynapseAdminClient mockClient;
	@Mock
	private AsyncMigrationIdRangeChecksumWorkerFactory mockWorkerFactory;
	@Mock
	private AsyncMigrationIdRangeChecksumWorker mockSourceWorker;
	@Mock
	private AsyncMigrationIdRangeChecksumWorker mockDestinationWorker;

	private ExecutorService threadPool;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);

		when(mockWorkerFactory.getSourceWorker(MigrationType.ACL, "salt", 0, 100)).thenReturn(mockSourceWorker);
		when(mockWorkerFactory.getDestinationWorker(MigrationType.ACL, "salt", 0, 100)).thenReturn(mockDestinationWorker);

		threadPool = Executors.newFixedThreadPool(1);
	}

	@Test
	public void testOK() throws Exception {
		// Expected from mockClient
		MigrationRangeChecksum expectedChecksum = new MigrationRangeChecksum();
		expectedChecksum.setType(MigrationType.ACL);
		expectedChecksum.setMinid(0L);
		expectedChecksum.setMaxid(100L);
		expectedChecksum.setChecksum("checksum");

		when(mockSourceWorker.call()).thenReturn(expectedChecksum);
		when(mockDestinationWorker.call()).thenReturn(expectedChecksum);

		// Expected results
		ConcurrentExecutionResult<MigrationRangeChecksum> expectedResults = new ConcurrentExecutionResult<MigrationRangeChecksum>();
		expectedResults.setSourceResult(expectedChecksum);
		expectedResults.setDestinationResult(expectedChecksum);

		ConcurrentMigrationIdRangeChecksumsExecutor executor = new ConcurrentMigrationIdRangeChecksumsExecutor(threadPool, mockWorkerFactory);

		// Call under test
		ConcurrentExecutionResult<MigrationRangeChecksum> results = executor.getIdRangeChecksums(MigrationType.ACL, "salt", 0, 100);

		assertEquals(expectedResults, results);

	}

	@Test(expected = AsyncMigrationException.class)
	public void testException() throws Exception {
		// Expected from mock
		TimeoutException timeoutException = new TimeoutException("Timeout");
		AsyncMigrationException expectedException = new AsyncMigrationException(timeoutException);

		when(mockSourceWorker.call()).thenThrow(expectedException);

		ConcurrentMigrationIdRangeChecksumsExecutor executor = new ConcurrentMigrationIdRangeChecksumsExecutor(threadPool, mockWorkerFactory);

		// Call under test
		ConcurrentExecutionResult<MigrationRangeChecksum> results = executor.getIdRangeChecksums(MigrationType.ACL, "salt", 0, 100);

	}

}