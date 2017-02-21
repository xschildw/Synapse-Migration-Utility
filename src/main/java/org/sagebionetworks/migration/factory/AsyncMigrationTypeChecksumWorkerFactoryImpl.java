package org.sagebionetworks.migration.factory;

import org.sagebionetworks.migration.async.AsyncMigrationTypeChecksumWorker;
import org.sagebionetworks.repo.model.migration.MigrationType;

import java.util.List;

public class AsyncMigrationTypeChecksumWorkerFactoryImpl implements AsyncMigrationTypeChecksumWorkerFactory {

	private SynapseClientFactory clientFactory;

	public AsyncMigrationTypeChecksumWorkerFactoryImpl(SynapseClientFactory clientFactory) {
		this.clientFactory = clientFactory;
	}

	@Override
	public AsyncMigrationTypeChecksumWorker getSourceWorker(MigrationType type, long timeoutMS) {
		AsyncMigrationTypeChecksumWorker worker = new AsyncMigrationTypeChecksumWorker(this.clientFactory.getSourceClient(), type, timeoutMS);
		return worker;
	}

	@Override
	public AsyncMigrationTypeChecksumWorker getDestinationWorker(MigrationType type, long timeoutMS) {
		AsyncMigrationTypeChecksumWorker worker = new AsyncMigrationTypeChecksumWorker(this.clientFactory.getDestinationClient(), type, timeoutMS);
		return worker;
	}
}
