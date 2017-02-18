package org.sagebionetworks.migration.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.repo.model.migration.AsyncMigrationTypeCountsRequest;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeCounts;

import java.util.List;
import java.util.concurrent.Callable;

public class AsyncMigrationTypeCountsWorker implements Callable<MigrationTypeCounts> {
    static private Logger logger = LogManager.getLogger(AsyncMigrationTypeCountsWorker.class);

    AsyncMigrationRequestExecutor reqExecutor;

    public AsyncMigrationTypeCountsWorker(SynapseAdminClient client, List<MigrationType> migrationTypes, long timeoutMS) {
        AsyncMigrationTypeCountsRequest asyncMigrationRequest = new AsyncMigrationTypeCountsRequest();
        asyncMigrationRequest.setTypes(migrationTypes);
        this.reqExecutor = new AsyncMigrationRequestExecutor(client, asyncMigrationRequest, timeoutMS);
    }

    @Override
    public MigrationTypeCounts call() throws Exception {
        AdminResponse response = reqExecutor.execute();
        if (! (response instanceof MigrationTypeCounts)) {
            throw(new IllegalArgumentException("Should not happen!"));
        } else {
            return (MigrationTypeCounts)response;
        }
    }
}
