package org.sagebionetworks.migration.factory;

import org.sagebionetworks.migration.async.AsyncMigrationTypeCountsWorker;
import org.sagebionetworks.repo.model.migration.MigrationType;

import java.util.List;

public class AsyncMigrationTypeCountsWorkerFactoryImpl implements AsyncMigrationTypeCountsWorkerFactory {

    private SynapseClientFactory clientFactory;
    private long timeoutMS;

    public AsyncMigrationTypeCountsWorkerFactoryImpl(SynapseClientFactory clientFactory, long timeoutMS) {
        this.clientFactory = clientFactory;
        this.timeoutMS = timeoutMS;
    }

    @Override
    public AsyncMigrationTypeCountsWorker getSourceWorker(List<MigrationType> types) {
        AsyncMigrationTypeCountsWorker worker = new AsyncMigrationTypeCountsWorker(this.clientFactory.getSourceClient(), types, this.timeoutMS);
        return worker;
    }

    @Override
    public AsyncMigrationTypeCountsWorker getDestinationWorker(List<MigrationType> types) {
        AsyncMigrationTypeCountsWorker worker = new AsyncMigrationTypeCountsWorker(this.clientFactory.getDestinationClient(), types, this.timeoutMS);
        return worker;
    }
}
