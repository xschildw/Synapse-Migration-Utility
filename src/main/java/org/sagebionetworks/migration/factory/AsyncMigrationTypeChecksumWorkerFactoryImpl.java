package org.sagebionetworks.migration.factory;

import org.sagebionetworks.migration.async.AsyncMigrationTypeChecksumWorker;
import org.sagebionetworks.repo.model.migration.MigrationType;

import java.util.List;

public class AsyncMigrationTypeChecksumWorkerFactoryImpl implements AsyncMigrationTypeChecksumWorkerFactory {

	private SynapseClientFactory clientFactory;
	private long timeoutMS;

	public AsyncMigrationTypeChecksumWorkerFactoryImpl(SynapseClientFactory clientFactory, long timeoutMS) {
		this.clientFactory = clientFactory;
		this.timeoutMS = timeoutMS;
	}

	@Override
	public AsyncMigrationTypeChecksumWorker getSourceWorker(MigrationType type) {
		AsyncMigrationTypeChecksumWorker worker = new AsyncMigrationTypeChecksumWorker(this.clientFactory.getSourceClient(), type, this.timeoutMS);
		return worker;
	}

	@Override
	public AsyncMigrationTypeChecksumWorker getDestinationWorker(MigrationType type) {
		AsyncMigrationTypeChecksumWorker worker = new AsyncMigrationTypeChecksumWorker(this.clientFactory.getDestinationClient(), type, this.timeoutMS);
		return worker;
	}
}
