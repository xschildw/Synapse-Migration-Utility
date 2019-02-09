package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.List;

import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.util.Clock;

import com.google.inject.Inject;

/**
 * This algorithm drives the migration process by finding all jobs to be
 * restored on the destination.
 * 
 */
public class MigrationDriverImpl implements MigrationDriver {

	Configuration config;
	DestinationJobBuilder jobBuilder;
	RestoreJobQueue restoreJobQueue;
	Clock clock;

	@Inject
	public MigrationDriverImpl(Configuration config, DestinationJobBuilder jobBuilder, RestoreJobQueue restoreJobQueue,
			Clock clock) {
		super();
		this.config = config;
		this.jobBuilder = jobBuilder;
		this.restoreJobQueue = restoreJobQueue;
		this.clock = clock;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.sagebionetworks.migration.async.AsynchronousMigration#migratePrimaryTypes
	 * (java.util.List)
	 */
	@Override
	public void migratePrimaryTypes(List<TypeToMigrateMetadata> primaryTypes) {
		// find all of the restore jobs as fast as possible.
		Iterator<DestinationJob> jobIterator = jobBuilder.buildDestinationJobs(primaryTypes);
		while (jobIterator.hasNext()) {
			DestinationJob nextJob = jobIterator.next();
			// push restore jobs the restore queue
			restoreJobQueue.pushJob(nextJob);
		}
		// Wait for all of the restore jobs to finish
		while (!restoreJobQueue.isDone()) {
			try {
				clock.sleep(1000L);
			} catch (InterruptedException e1) {
				// interrupt will trigger failure.
				throw new RuntimeException(e1);
			}
		}
		AsyncMigrationException lastException = restoreJobQueue.getLastException();
		if (lastException != null) {
			throw lastException;
		}
	}

}
