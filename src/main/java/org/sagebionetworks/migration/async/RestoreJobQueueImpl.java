package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.logging.log4j.Logger;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.LoggerFactory;
import org.sagebionetworks.repo.model.migration.MigrationType;

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
public class RestoreJobQueueImpl implements RestoreJobQueue, Runnable {

	DestinationJobExecutor jobExecutor;
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
		boolean isDone = jobWaitingQueue.isEmpty() && runningJobs.isEmpty();
		// When all jobs are done throw the last exception if one exists.
		if(isDone && lastException != null) {
			throw lastException;
		}
		return isDone;
	}

	/**
	 * Called each time the timer is fired. Note: This method is called from the
	 * timer thread.
	 */
	void timerFired() {
		/*
		 * Check on all of the running jobs. Finished or failed jobs will be removed.
		 * Status of all running jobs will be reported to the log.
		 */
		removeAllFinishedJobs();
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
	void removeAllFinishedJobs() {
		// remove all finished jobs
		Iterator<Future<?>> runningItertor = runningJobs.values().iterator();
		while (runningItertor.hasNext()) {
			Future<?> future = runningItertor.next();
			try {
				// check if this job is done.
				if (future.isDone()) {
					// A call to get() is needed to check if the job failed. PLFM-5430.
					future.get();
					runningItertor.remove();
				}
			} catch (AsyncMigrationException | InterruptedException | ExecutionException e) {
				// track the last seen exception.
				lastException = new AsyncMigrationException(e);
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
			boolean canJobStart = false;
			if(MigrationType.CHANGE.equals(job.getMigrationType())) {
				// Change jobs can only be run if no other jobs are running.
				if(this.runningJobs.isEmpty()) {
					canJobStart = true;
				}
			}else {
				/*
				 * Non-change jobs can run as long as no other job of the 
				 * same time is already running
				 */
				if (!runningJobs.containsKey(job.getMigrationType())) {
					canJobStart = true;
				}
			}
			if(canJobStart) {
				// Start a job and add it to the queue
				Future<?> future = jobExecutor.startDestinationJob(job);
				this.runningJobs.put(job.getMigrationType(), future);
				queuIterator.remove();
			}
		}
	}

	@Override
	public synchronized void run() {
		this.timerFired();
	}

}
