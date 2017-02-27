package org.sagebionetworks.migration.factory;

import org.sagebionetworks.migration.async.AsyncMigrationIdRangeChecksumWorker;
import org.sagebionetworks.repo.model.migration.MigrationType;

public class AsyncMigrationIdRangeChecksumWorkerFactoryImpl implements AsyncMigrationIdRangeChecksumWorkerFactory {

	private SynapseClientFactory clientFactory;

	public AsyncMigrationIdRangeChecksumWorkerFactoryImpl (SynapseClientFactory clientFactory) {
		this.clientFactory = clientFactory;
	}

	@Override
	public AsyncMigrationIdRangeChecksumWorker getSourceWorker(MigrationType type, String salt, long minId, long maxId, long timeoutMS) {
		AsyncMigrationIdRangeChecksumWorker worker = new AsyncMigrationIdRangeChecksumWorker(clientFactory.getSourceClient(), type, salt, minId, maxId, timeoutMS);
		return worker;
	}

	@Override
	public AsyncMigrationIdRangeChecksumWorker getDestinationWorker(MigrationType type, String salt, long minId, long maxId, long timeoutMS) {
		AsyncMigrationIdRangeChecksumWorker worker = new AsyncMigrationIdRangeChecksumWorker(clientFactory.getDestinationClient(), type, salt, minId, maxId, timeoutMS);
		return worker;
	}
}
