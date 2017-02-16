package org.sagebionetworks.migration.async;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.repo.model.asynch.AsynchJobState;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.*;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class ConcurrentMigrationTypeCountsExecutorTest {

    @Mock
    private SynapseAdminClient mockClient;
    @Mock
    private SynapseClientFactory mockFactory;
    private ExecutorService threadPool;

    @Before
    public void setUp() {

        MockitoAnnotations.initMocks(this);
        when(mockFactory.getSourceClient()).thenReturn(mockClient);
        when(mockFactory.getDestinationClient()).thenReturn(mockClient);
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

        when(mockClient.startAdminAsynchronousJob(any(AsyncMigrationRequest.class))).thenReturn(expectedStatus, expectedStatus);
        when(mockClient.getAdminAsynchronousJobStatus("1")).thenReturn(expectedStatus, expectedStatus);

        ConcurrentMigrationTypeCountsExecutor executor = new ConcurrentMigrationTypeCountsExecutor(threadPool, mockFactory, types, 100);

        // Call under test
        ConcurrentExecutionResult<List<MigrationTypeCount>> concTypeCounts = executor.getMigrationTypeCounts();

        assertNotNull(concTypeCounts);
        assertNotNull(concTypeCounts.getSourceResult());
        assertNotNull(concTypeCounts.getDestinationResult());

    }

}