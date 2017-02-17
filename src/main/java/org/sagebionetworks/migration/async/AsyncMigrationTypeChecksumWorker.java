package org.sagebionetworks.migration.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.repo.model.migration.AsyncMigrationTypeChecksumRequest;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeChecksum;

import java.util.concurrent.Callable;

public class AsyncMigrationTypeChecksumWorker implements Callable<MigrationTypeChecksum> {
    static private Logger logger = LogManager.getLogger(AsyncMigrationTypeChecksumWorker.class);

    private AsyncMigrationWorker worker;

    public AsyncMigrationTypeChecksumWorker(SynapseAdminClient client, MigrationType type, long timeoutMS) {
        AsyncMigrationTypeChecksumRequest request = new AsyncMigrationTypeChecksumRequest();
        request.setType(type.name());
        this.worker = new AsyncMigrationWorker(client, request, timeoutMS);
    }

    @Override
    public MigrationTypeChecksum call() {
        AdminResponse response = worker.call();
        if (! (response instanceof MigrationTypeChecksum)) {
            throw(new IllegalArgumentException("Should not happen!"));
        } else {
            return (MigrationTypeChecksum)response;
        }
    }
}
