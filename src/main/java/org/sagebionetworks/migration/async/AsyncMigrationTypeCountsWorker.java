package org.sagebionetworks.migration.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.repo.model.migration.AsyncMigrationTypeCountsRequest;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeCounts;
import org.sagebionetworks.tool.progress.BasicProgress;

import java.util.List;
import java.util.concurrent.Callable;

public class AsyncMigrationTypeCountsWorker implements Callable<MigrationTypeCounts> {
    static private Logger logger = LogManager.getLogger(AsyncMigrationTypeCountsWorker.class);

    private SynapseAdminClientImpl client;
    private List<MigrationType> migrationTypes;
    private long timeoutMS;

    public AsyncMigrationTypeCountsWorker(SynapseAdminClient client, List<MigrationType> migrationTypes, long timeoutMS) {
        this.client = (SynapseAdminClientImpl)client;
        this.migrationTypes = migrationTypes;
        this.timeoutMS = timeoutMS;
    }

    @Override
    public MigrationTypeCounts call() throws Exception {
        AsyncMigrationTypeCountsRequest req = new AsyncMigrationTypeCountsRequest();
        req.setTypes(this.migrationTypes);
        AsyncMigrationWorker worker = new AsyncMigrationWorker(this.client, req, this.timeoutMS);
        MigrationTypeCounts counts = null;
        AdminResponse response = worker.call();
        if (! (response instanceof MigrationTypeCounts)) {
            throw(new IllegalArgumentException("Should not happen!"));
        } else {
            return (MigrationTypeCounts)response;
        }
    }
}
