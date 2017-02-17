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
import org.sagebionetworks.repo.model.migration.AsyncMigrationRequest;
import org.sagebionetworks.repo.model.migration.AsyncMigrationResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeChecksum;

import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class AsyncMigrationTypeChecksumWorkerTest {

    @Mock
    private SynapseAdminClient mockClient;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testOK() throws Exception {
        // Expected from mockClient
        AsynchronousJobStatus expectedStatus = new AsynchronousJobStatus();
        expectedStatus.setJobId("1");
        expectedStatus.setJobState(AsynchJobState.COMPLETE);
        MigrationTypeChecksum expectedTypeChecksum = new MigrationTypeChecksum();
        expectedTypeChecksum.setType(MigrationType.ACL);
        expectedTypeChecksum.setChecksum("checksum");
        AsyncMigrationResponse expectedResponse = new AsyncMigrationResponse();
        expectedResponse.setAdminResponse(expectedTypeChecksum);
        expectedStatus.setResponseBody(expectedResponse);

        when(mockClient.startAdminAsynchronousJob(any(AsyncMigrationRequest.class))).thenReturn(expectedStatus);
        when(mockClient.getAdminAsynchronousJobStatus("1")).thenReturn(expectedStatus);

        AsyncMigrationTypeChecksumWorker worker = new AsyncMigrationTypeChecksumWorker(mockClient, MigrationType.ACL, 1000);

        // Call under test
        MigrationTypeChecksum typeChecksum = worker.call();
        assertNotNull(typeChecksum);
        assertEquals(expectedTypeChecksum, typeChecksum);
    }

    @Test(expected = AsyncMigrationException.class)
    public void testException() throws Exception {
        // Expected exception
        TimeoutException timeoutException = new TimeoutException("Timed out waiting for the job to complete");
        AsyncMigrationException expectedException = new AsyncMigrationException(timeoutException);
        when(mockClient.startAdminAsynchronousJob(any(AsyncMigrationRequest.class))).thenThrow(expectedException);

        AsyncMigrationTypeChecksumWorker worker = new AsyncMigrationTypeChecksumWorker(mockClient, MigrationType.ACL, 1000);

        // Call under test
        MigrationTypeChecksum checksum = worker.call();
    }

}