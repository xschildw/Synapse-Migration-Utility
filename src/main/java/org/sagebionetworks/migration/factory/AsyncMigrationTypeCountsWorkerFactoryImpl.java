package org.sagebionetworks.migration.factory;

import org.sagebionetworks.migration.async.AsyncMigrationTypeCountsWorker;
import org.sagebionetworks.repo.model.migration.MigrationType;

import java.util.List;

public class AsyncMigrationTypeCountsWorkerFactoryImpl implements AsyncMigrationTypeCountsWorkerFactory {

    private SynapseClientFactory clientFactory;

    public AsyncMigrationTypeCountsWorkerFactoryImpl(SynapseClientFactory client) {
        this.clientFactory = client;
    }

    @Override
    public AsyncMigrationTypeCountsWorker getSourceWorker(List<MigrationType> types, long timeoutMS) {
        AsyncMigrationTypeCountsWorker worker = new AsyncMigrationTypeCountsWorker(this.clientFactory.getSourceClient(), types, timeoutMS);
        return worker;
    }

    @Override
    public AsyncMigrationTypeCountsWorker getDestinationWorker(List<MigrationType> types, long timeoutMS) {
        AsyncMigrationTypeCountsWorker worker = new AsyncMigrationTypeCountsWorker(this.clientFactory.getDestinationClient(), types, timeoutMS);
        return worker;
    }
}
