package org.sagebionetworks.migration.async;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.factory.AsyncMigrationTypeCountsWorkerFactory;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.repo.model.asynch.AsynchJobState;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.*;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

public class ConcurrentMigrationTypeCountsExecutorTest {

    @Mock
    private SynapseAdminClient mockClient;
    @Mock
    private AsyncMigrationTypeCountsWorkerFactory mockFactory;
    @Mock
    private AsyncMigrationTypeCountsWorker mockSourceWorker;
    @Mock
    private AsyncMigrationTypeCountsWorker mockDestinationWorker;
/*
    @Mock
    private Future<MigrationTypeCounts> mockFutureSourceTypeCounts;
    @Mock
    private Future<MigrationTypeCounts> mockFutureDestinationTypeCounts;
*/

    private ExecutorService threadPool;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        // Some types
        List<MigrationType> types = new LinkedList<MigrationType>();
        types.add(MigrationType.ACCESS_APPROVAL);

        when(mockFactory.getSourceWorker(types)).thenReturn(mockSourceWorker);
        when(mockFactory.getDestinationWorker(types)).thenReturn(mockDestinationWorker);

        threadPool = Executors.newFixedThreadPool(1);
    }

    @After
    public void tearDown() {

    }

    @Test
    public void testOK() throws Exception {
        // Some types
        List<MigrationType> types = new LinkedList<MigrationType>();
        types.add(MigrationType.ACCESS_APPROVAL);

        // Expected from mock
        MigrationTypeCounts expectedTypeCounts = new MigrationTypeCounts();
        List<MigrationTypeCount> l = new LinkedList<MigrationTypeCount>();
        MigrationTypeCount tc1 = new MigrationTypeCount();
        tc1.setType(MigrationType.ACCESS_APPROVAL);
        tc1.setCount(10L);
        l.add(tc1);
        expectedTypeCounts.setList(l);

        when(mockSourceWorker.call()).thenReturn(expectedTypeCounts);
        when(mockDestinationWorker.call()).thenReturn(expectedTypeCounts);

        // Expected result
        ResultPair<List<MigrationTypeCount>> expectedResult = new ResultPair<List<MigrationTypeCount>>();
        expectedResult.setSourceResult(l);
        expectedResult.setDestinationResult(l);

        ConcurrentMigrationTypeCountsExecutor executor = new ConcurrentMigrationTypeCountsExecutor(threadPool, mockFactory);

        // Call under test
        ResultPair<List<MigrationTypeCount>> concTypeCounts = executor.getMigrationTypeCounts(types);

        assertNotNull(concTypeCounts);
        assertEquals(expectedResult, concTypeCounts);

    }

    @Test(expected = AsyncMigrationException.class)
    public void testException() throws Exception {
        // Some types
        List<MigrationType> types = new LinkedList<MigrationType>();
        types.add(MigrationType.ACCESS_APPROVAL);

        // Expected from mock
        TimeoutException timeoutException = new TimeoutException("Timeout");
        AsyncMigrationException expectedException = new AsyncMigrationException(timeoutException);


        when(mockSourceWorker.call()).thenThrow(expectedException);

        ConcurrentMigrationTypeCountsExecutor executor = new ConcurrentMigrationTypeCountsExecutor(threadPool, mockFactory);

        // Call under test
        ResultPair<List<MigrationTypeCount>> concTypeCounts = executor.getMigrationTypeCounts(types);
    }

}