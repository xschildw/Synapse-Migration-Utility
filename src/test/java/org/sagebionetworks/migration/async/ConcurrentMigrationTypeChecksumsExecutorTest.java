package org.sagebionetworks.migration.async;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.factory.AsyncMigrationTypeChecksumWorkerFactory;
import org.sagebionetworks.migration.factory.AsyncMigrationTypeCountsWorkerFactory;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeChecksum;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class ConcurrentMigrationTypeChecksumsExecutorTest {
    @Mock
    private SynapseAdminClient mockClient;
    @Mock
    private AsyncMigrationTypeChecksumWorkerFactory mockFactory;
    @Mock
    private AsyncMigrationTypeChecksumWorker mockSourceWorker;
    @Mock
    private AsyncMigrationTypeChecksumWorker mockDestinationWorker;

    private ExecutorService threadPool;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(mockFactory.getSourceWorker(MigrationType.ACL)).thenReturn(mockSourceWorker);
        when(mockFactory.getDestinationWorker(MigrationType.ACL)).thenReturn(mockDestinationWorker);

        threadPool = Executors.newFixedThreadPool(1);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testOK() throws Exception {
        //Expected from mock
        MigrationTypeChecksum expectedChecksum = new MigrationTypeChecksum();
        expectedChecksum.setType(MigrationType.ACL);
        expectedChecksum.setChecksum("checksum");

        when(mockSourceWorker.call()).thenReturn(expectedChecksum);
        when(mockDestinationWorker.call()).thenReturn(expectedChecksum);

        // Expected results
        ConcurrentExecutionResult<MigrationTypeChecksum> expectedResults = new ConcurrentExecutionResult<MigrationTypeChecksum>();
        expectedResults.setSourceResult(expectedChecksum);
        expectedResults.setDestinationResult(expectedChecksum);

        ConcurrentMigrationTypeChecksumsExecutor executor = new ConcurrentMigrationTypeChecksumsExecutor(threadPool, mockFactory);

        // Call under test
        ConcurrentExecutionResult<MigrationTypeChecksum> results = executor.getMigrationTypeChecksums(MigrationType.ACL);

        assertNotNull(results);
        assertEquals(expectedResults, results);

    }

    @Test(expected = AsyncMigrationException.class)
    public void testException() throws Exception {
        // Expected from mock
        TimeoutException timeoutException = new TimeoutException("Timeout");
        AsyncMigrationException expectedException = new AsyncMigrationException(timeoutException);

        when(mockSourceWorker.call()).thenThrow(expectedException);

        ConcurrentMigrationTypeChecksumsExecutor executor = new ConcurrentMigrationTypeChecksumsExecutor(threadPool, mockFactory);

        // Call under test
        ConcurrentExecutionResult<MigrationTypeChecksum> checksums = executor.getMigrationTypeChecksums(MigrationType.ACL);

    }

}