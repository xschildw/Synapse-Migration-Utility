package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.logging.log4j.Logger;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.LoggerFactory;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.util.Clock;

/**
 * DestinationJob of the same MigrationType are run in sequentially, while jobs
 * of different MigrationType are run concurrently.
 * 
 * We do not run jobs of the same type concurrently because they will fail with
 * lock timeout or deadlock exceptions.
 * 
 * The public methods of this class are called from two separate threads; the
 * main thread and the timer thread. All public methods are synchronized to
 * ensure consistency between the two thread.
 *
 */
public class RestoreJobQueueImpl implements RestoreJobQueue {

	DestinationJobExecutor jobExecutor;
	Clock clock;
	Logger logger;
	/*
	 * The queue of jobs waiting to be started.
	 */
	List<DestinationJob> jobWaitingQueue;
	/*
	 * Mapping of the running jobs by MigrationType.
	 */
	Map<MigrationType, Future<?>> runningJobs;
	AsyncMigrationException lastException;

	/**
	 * Create a new queue. The caller must all call timerFired() from a timer
	 * thread.
	 * 
	 * @param jobExecutor
	 * @param loggerFactory
	 */
	public RestoreJobQueueImpl(DestinationJobExecutor jobExecutor, LoggerFactory loggerFactory) {
		this.jobExecutor = jobExecutor;
		this.logger = loggerFactory.getLogger(RestoreJobQueueImpl.class);
		jobWaitingQueue = new LinkedList<>();
		runningJobs = new LinkedHashMap<>(MigrationType.values().length);
	}

	/**
	 * Push a job to the waiting job queue. Note: This method will be called from
	 * the main thread.
	 */
	@Override
	public synchronized void pushJob(DestinationJob job) {
		// Add the job to the wait queue.
		jobWaitingQueue.add(job);
	}

	/**
	 * Check if all of the queued jobs are started and completed. Note: This method
	 * is called from the main thread.
	 */
	@Override
	public synchronized boolean isDone() {
		return jobWaitingQueue.isEmpty() && runningJobs.isEmpty();
	}

	/**
	 * Called each time the timer is fired. Note: This method is called from the
	 * timer thread.
	 */
	public synchronized void timerFired() {
		/*
		 * Check on all of the running jobs. Finished or failed jobs will be removed.
		 * Status of all running jobs will be reported to the log.
		 */
		checkOnRunningJobs();
		/*
		 * Start all jobs that currently do not have
		 */
		startEligibleJobs();

		logger.info("Currently running: " + runningJobs.size() + " restore jobs.  Waiting to start "
				+ jobWaitingQueue.size() + " restore jobs.");
	}

	/**
	 * Check on all of the running jobs. Finished jobs and failed jobs are removed.
	 * Calling this method will trigger each running job to report its current
	 * status to the log.
	 */
	void checkOnRunningJobs() {
		// remove all finished jobs
		Iterator<Future<?>> runningItertor = runningJobs.values().iterator();
		while (runningItertor.hasNext()) {
			Future<?> future = runningItertor.next();
			try {
				// check if this job is done.
				if (future.isDone()) {
					runningItertor.remove();
				}
			} catch (AsyncMigrationException e) {
				// track the last seen exception.
				lastException = e;
				runningItertor.remove();
			}
		}
	}

	/**
	 * A restore job can be started as long as a job of the same MigrationType is
	 * not already running.
	 */
	void startEligibleJobs() {
		Iterator<DestinationJob> queuIterator = jobWaitingQueue.iterator();
		while (queuIterator.hasNext()) {
			DestinationJob job = queuIterator.next();
			// do we already have job of this type running?
			Future<?> future = this.runningJobs.get(job.getMigrationType());
			if (future == null) {
				// Start a job and add it to the queue
				future = jobExecutor.startDestinationJob(job);
				this.runningJobs.put(job.getMigrationType(), future);
				queuIterator.remove();
			}
		}
	}

	@Override
	public synchronized AsyncMigrationException getLastException() {
		return lastException;
	}

}
