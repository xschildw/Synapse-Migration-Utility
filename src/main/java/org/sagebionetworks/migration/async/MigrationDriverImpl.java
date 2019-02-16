package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.List;

import org.sagebionetworks.migration.async.checksum.ChecksumDeltaBuilder;
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

	public static final long SLEEP_TIME_MS = 1000L;
	Configuration config;
	MissingFromDestinationBuilder missingFromDestinationBuilder;
	ChecksumDeltaBuilder checksumChangeBuilder;
	RestoreJobQueue restoreJobQueue;
	Clock clock;

	@Inject
	public MigrationDriverImpl(Configuration config, MissingFromDestinationBuilder missingFromDestinationBuilder,
			ChecksumDeltaBuilder checksumChangeBuilder, RestoreJobQueue restoreJobQueue, Clock clock) {
		super();
		this.config = config;
		this.missingFromDestinationBuilder = missingFromDestinationBuilder;
		this.checksumChangeBuilder = checksumChangeBuilder;
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
		/*
		 * Phase One: Find and process all data that is missing from the destination.
		 */
		findAndProcessJobs(missingFromDestinationBuilder.buildDestinationJobs(primaryTypes));
		/*
		 * Phase Two: Find and process all remaining deltas between source and
		 * destination by comparing checkums.
		 */
		findAndProcessJobs(checksumChangeBuilder.buildAllRestoreJobsForMismatchedChecksums(primaryTypes));
	}

	/**
	 * Find and process all jobs from the provided job iterator.
	 * @param jobIterator
	 */
	void findAndProcessJobs(Iterator<DestinationJob> jobIterator) {
		// find all of the restore jobs as fast as possible.
		while (jobIterator.hasNext()) {
			DestinationJob nextJob = jobIterator.next();
			// push restore jobs the restore queue
			restoreJobQueue.pushJob(nextJob);
		}
		// Wait for all of the restore jobs to finish
		while (!restoreJobQueue.isDone()) {
			try {
				clock.sleep(SLEEP_TIME_MS);
			} catch (InterruptedException e1) {
				// interrupt will trigger failure.
				throw new RuntimeException(e1);
			}
		}
	}

}
