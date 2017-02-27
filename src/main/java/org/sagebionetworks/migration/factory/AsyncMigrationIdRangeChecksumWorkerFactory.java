package org.sagebionetworks.migration.factory;

import org.sagebionetworks.migration.async.AsyncMigrationIdRangeChecksumWorker;
import org.sagebionetworks.repo.model.migration.MigrationType;

public interface AsyncMigrationIdRangeChecksumWorkerFactory {
	public AsyncMigrationIdRangeChecksumWorker getSourceWorker(MigrationType type, String salt, long minId, long maxId);
	public AsyncMigrationIdRangeChecksumWorker getDestinationWorker(MigrationType type, String salt, long minId, long maxId);
}
