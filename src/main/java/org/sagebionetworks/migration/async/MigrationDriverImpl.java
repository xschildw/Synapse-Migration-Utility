package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.util.Clock;

import com.google.inject.Inject;

/**
 * This algorithm drives the migration process by pulling in changes to be
 * migrated from the source and pushing them to the destination. Changes
 * are pulled from the source in series but they are pushed to the destination in
 * parallel.
 */
public class MigrationDriverImpl implements MigrationDriver {

	// Migration can run 1000s of jobs that take < 1s, we should not spend too much time waiting
	static final long SLEEP_TIME_MS = 100L;

	Configuration config;
	DestinationJobBuilder jobBuilder;
	DestinationJobExecutor jobExecutor;
	Clock clock;

	@Inject
	public MigrationDriverImpl(Configuration config, DestinationJobBuilder jobBuilder,
			DestinationJobExecutor jobExecutor, Clock clock) {
		super();
		this.config = config;
		this.jobBuilder = jobBuilder;
		this.jobExecutor = jobExecutor;
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
		AsyncMigrationException lastException = null;
		// Tracks the active destination jobs.
		List<Future<?>> activeDestinationJobs = new LinkedList<>();
		// Start generating jobs to push to the destination.
		Iterator<DestinationJob> jobIterator = jobBuilder.buildDestinationJobs(primaryTypes);
		while (jobIterator.hasNext()) {
			DestinationJob nextJob = jobIterator.next();
			// Start this job on the destination.
			activeDestinationJobs.add(jobExecutor.startDestinationJob(nextJob));
			// wait for the number of active destination jobs to be under the max
			while (activeDestinationJobs.size() >= config.getMaximumNumberOfDestinationJobs()) {
				// give the jobs some time to complete
				sleep();
				try {
					// remove any completed jobs
					removeTerminatedJobs(activeDestinationJobs);
				} catch (AsyncMigrationException e) {
					lastException = e;
				}
			}
		}
		try {
			// wait for any remaining jobs to complete.
			waitForJobsToTerminate(activeDestinationJobs);
		} catch (AsyncMigrationException e) {
			lastException = e;
		}
		if (lastException != null) {
			// throwing this exception signals another migration run is required.
			throw lastException;
		}
	}

	/**
	 * Wait for all of the given jobs to terminate.
	 * 
	 * @param activeDestinationJobs
	 */
	void waitForJobsToTerminate(List<Future<?>> activeDestinationJobs) {
		AsyncMigrationException lastException = null;
		for (Future<?> future : activeDestinationJobs) {
			try {
				getJobResults(future);
			} catch (AsyncMigrationException e) {
				lastException = e;
			}
		}
		if (lastException != null) {
			throw lastException;
		}
	}

	/**
	 * Sleep for two seconds.
	 */
	void sleep() {
		try {
			clock.sleep(SLEEP_TIME_MS);
		} catch (InterruptedException e1) {
			// interrupt will trigger failure.
			throw new RuntimeException(e1);
		}
	}

	/**
	 * Remove any job that has terminated either with success or failure.
	 * 
	 * @param lastException
	 * @param activeDestinationJobs
	 * @return
	 */
	void removeTerminatedJobs(List<Future<?>> activeDestinationJobs) {
		AsyncMigrationException lastException = null;
		// find an remove any completed jobs.
		Iterator<Future<?>> futureIterator = activeDestinationJobs.iterator();
		while (futureIterator.hasNext()) {
			Future<?> jobFuture = futureIterator.next();
			try {
				if (jobFuture.isDone()) {
					getJobResults(jobFuture);
					// remove complete jobs
					futureIterator.remove();
				}
			} catch (AsyncMigrationException e) {
				// removed failed jobs
				futureIterator.remove();
				lastException = e;
			}
		}
		if (lastException != null) {
			throw lastException;
		}
	}

	/**
	 * Get the job results
	 * 
	 * @param jobFuture
	 */
	void getJobResults(Future<?> jobFuture) {
		try {
			// call get to determine if the job succeeded.
			jobFuture.get();
		} catch (AsyncMigrationException e) {
			// Indicates that another migration run is required without terminating this
			// run.
			throw e;
		} catch (Exception e) {
			// any other type of exception will terminate the migration.
			throw new RuntimeException(e);
		}
	}

}
