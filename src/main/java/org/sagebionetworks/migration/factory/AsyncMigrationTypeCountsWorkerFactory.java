package org.sagebionetworks.migration.factory;

import org.sagebionetworks.migration.async.AsyncMigrationTypeCountsWorker;
import org.sagebionetworks.repo.model.migration.MigrationType;

import java.util.List;

public interface AsyncMigrationTypeCountsWorkerFactory {

    public AsyncMigrationTypeCountsWorker getSourceWorker(List<MigrationType> types, long timeoutMS);
    public AsyncMigrationTypeCountsWorker getDestinationWorker(List<MigrationType> types, long timeoutMS);
}
