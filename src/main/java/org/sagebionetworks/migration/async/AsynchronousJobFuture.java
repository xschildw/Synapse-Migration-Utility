package org.sagebionetworks.migration.async;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.Reporter;
import org.sagebionetworks.repo.model.asynch.AsynchJobState;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.repo.model.migration.AsyncMigrationResponse;
import org.sagebionetworks.util.Clock;

/**
 * A future to track an Asynchronous Job.
 *
 * @param <O> Response type.
 */
public class AsynchronousJobFuture<O extends AdminResponse> implements Future<O> {

	static final int SLEEP_TIME = 2000;
	static final String TIMEOUT_MESSAGE = "Timeout waiting for asynchronous job.";
	
	Reporter reporter;
	Clock clock;
	AsynchronousJobStatus jobStatus;
	SynapseAdminClient client;
	JobTarget jobTarget;
	String jobName;
	long defaultTimeoutMS;

	/**
	 * Create a future to track a started job.
	 * 
	 * @param jobStatus
	 * @param client
	 * @param reponseClass
	 */
	public AsynchronousJobFuture(Reporter reporter, Clock clock, AsynchronousJobStatus jobStatus, JobTarget jobTarget, SynapseAdminClient client,
			long defaultTimeoutMS) {
		this.reporter = reporter;
		this.clock = clock;
		this.jobStatus = jobStatus;
		this.jobTarget = jobTarget;
		this.client = client;
		this.defaultTimeoutMS = defaultTimeoutMS;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		// currently asynch jobs cannot be canceled.
		return false;
	}

	@Override
	public boolean isCancelled() {
		// currently asynch jobs cannot be canceled.
		return false;
	}

	@Override
	public boolean isDone() {
		try {
			if (AsynchJobState.PROCESSING == this.jobStatus.getJobState()) {
				// fetch the current status
				this.jobStatus = this.client.getAdminAsynchronousJobStatus(this.jobStatus.getJobId());
			}
			// a job is done if it is not processing.
			return AsynchJobState.PROCESSING != this.jobStatus.getJobState();
		} catch (Exception e) {
			throw new AsyncMigrationException(e);
		}
	}

	@Override
	public O get() throws InterruptedException, ExecutionException {
		try {
			return get(defaultTimeoutMS, TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			throw new AsyncMigrationException(e);
		}
	}

	@Override
	public O get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		// convert the timeout to timeout in MS.
		timeout = unit.toMillis(timeout);
		long start = clock.currentTimeMillis();
		// wait for the job to complete.
		while (!isDone()) {
			long elapse = clock.currentTimeMillis() - start;
			if (elapse > timeout) {
				// must be a timeout exception so callers can handle this case.
				throw new TimeoutException(TIMEOUT_MESSAGE);
			}
			reporter.reportProgress(jobTarget, jobStatus);
			// wait for the job.
			clock.sleep(SLEEP_TIME);
		}
		// extract the results
		switch (this.jobStatus.getJobState()) {
		case FAILED:
			throw new AsyncMigrationException("Job failed: " + this.jobStatus.getErrorMessage());
		case COMPLETE:
			AsyncMigrationResponse reponse = (AsyncMigrationResponse) this.jobStatus.getResponseBody();
			return (O) reponse.getAdminResponse();
		default:
			throw new RuntimeException("Unknown type: " + this.jobStatus.getJobState());
		}
	}

}
