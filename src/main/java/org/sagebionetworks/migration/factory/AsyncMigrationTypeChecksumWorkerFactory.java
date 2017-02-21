package org.sagebionetworks.migration.factory;

import org.sagebionetworks.migration.async.AsyncMigrationTypeChecksumWorker;
import org.sagebionetworks.repo.model.migration.MigrationType;

public interface AsyncMigrationTypeChecksumWorkerFactory {
	public AsyncMigrationTypeChecksumWorker getSourceWorker(MigrationType type, long timeoutMS);
	public AsyncMigrationTypeChecksumWorker getDestinationWorker(MigrationType type, long timeoutMS);
}
