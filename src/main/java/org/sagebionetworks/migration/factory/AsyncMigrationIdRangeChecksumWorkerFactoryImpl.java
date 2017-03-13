package org.sagebionetworks.migration.factory;

import org.sagebionetworks.migration.async.AsyncMigrationIdRangeChecksumWorker;
import org.sagebionetworks.repo.model.migration.MigrationType;

public class AsyncMigrationIdRangeChecksumWorkerFactoryImpl implements AsyncMigrationIdRangeChecksumWorkerFactory {

	private SynapseClientFactory clientFactory;
	private long timeoutMS;

	public AsyncMigrationIdRangeChecksumWorkerFactoryImpl (SynapseClientFactory clientFactory, long timeoutMS) {
		this.clientFactory = clientFactory;
		this.timeoutMS = timeoutMS;
	}

	@Override
	public AsyncMigrationIdRangeChecksumWorker getSourceWorker(MigrationType type, String salt, long minId, long maxId) {
		AsyncMigrationIdRangeChecksumWorker worker = new AsyncMigrationIdRangeChecksumWorker(clientFactory.getSourceClient(), type, salt, minId, maxId, this.timeoutMS);
		return worker;
	}

	@Override
	public AsyncMigrationIdRangeChecksumWorker getDestinationWorker(MigrationType type, String salt, long minId, long maxId) {
		AsyncMigrationIdRangeChecksumWorker worker = new AsyncMigrationIdRangeChecksumWorker(clientFactory.getDestinationClient(), type, salt, minId, maxId, this.timeoutMS);
		return worker;
	}
}
