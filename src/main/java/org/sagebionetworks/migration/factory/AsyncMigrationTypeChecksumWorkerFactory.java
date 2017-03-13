package org.sagebionetworks.migration.factory;

import org.sagebionetworks.migration.async.AsyncMigrationTypeChecksumWorker;
import org.sagebionetworks.repo.model.migration.MigrationType;

public interface AsyncMigrationTypeChecksumWorkerFactory {
	public AsyncMigrationTypeChecksumWorker getSourceWorker(MigrationType type);
	public AsyncMigrationTypeChecksumWorker getDestinationWorker(MigrationType type);
}
