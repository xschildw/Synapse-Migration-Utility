package org.sagebionetworks.migration.async;

import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.WorkerFailedException;
import org.sagebionetworks.repo.model.asynch.AsynchJobState;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.asynch.AsynchronousResponseBody;
import org.sagebionetworks.repo.model.migration.AdminRequest;
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.repo.model.migration.AsyncMigrationRequest;
import org.sagebionetworks.repo.model.migration.AsyncMigrationResponse;
import org.sagebionetworks.util.Clock;
import org.sagebionetworks.util.DefaultClock;

public class AsyncMigrationRequestExecutor {

	public static final int MAX_RETRY = 3;
	static private Logger logger = LogManager.getLogger(AsyncMigrationRequestExecutor.class);

	private SynapseAdminClient client;
	private AdminRequest request;
	long timeoutMs;
	private Clock clock;

	public AsyncMigrationRequestExecutor(SynapseAdminClient client, AdminRequest request, long timeoutMs) {
		this.client = client;
		this.request = request;
		this.timeoutMs = timeoutMs;
		this.clock = new DefaultClock();
	}

	public void setClock(Clock clock) {
		this.clock = clock;
	}

	public AdminResponse execute() throws AsyncMigrationException, InterruptedException {
		AsynchronousResponseBody resp = null;
		try {
			AsyncMigrationRequest migRequest = new AsyncMigrationRequest();
			migRequest.setAdminRequest(request);
			AsynchronousJobStatus status = client.startAdminAsynchronousJob(migRequest);
			status = waitForJobToCompleteWithRetry(status.getJobId(), MAX_RETRY);
			resp = status.getResponseBody();
			if (! (resp instanceof AsyncMigrationResponse)) {
				throw new IllegalArgumentException("Response from job " + status.getJobId() + " should be AsyncMigrationResponse!");
			}
		} catch (TimeoutException | WorkerFailedException | SynapseException | IllegalArgumentException e) {
			AsyncMigrationException e2 = new AsyncMigrationException(e.getMessage(), e);
			throw e2;
		}
		return ((AsyncMigrationResponse)resp).getAdminResponse();
	}
	
	private AsynchronousJobStatus waitForJobToCompleteWithRetry(String jobId, int maxRetry) throws WorkerFailedException, TimeoutException, InterruptedException {
		int numRetry = 0;
		AsynchronousJobStatus jobStatus;
		while (true) {
			numRetry++;
			try {
				jobStatus = waitForJobToComplete(jobId);
				break;
			} catch (TimeoutException e) {
				throw(e);
			} catch (WorkerFailedException e) {
				throw(e);
			} catch (SynapseException e) {
				if (numRetry >= maxRetry) {
					logger.debug("Too many retries in waitForJobToCompleteWithRetry()");
					throw new AsyncMigrationException("AsyncMigrationRequestExecutor: number of retries exceeded.", e);
				}
				clock.sleep(1000);
			}
		}
		return jobStatus;
	}

	private AsynchronousJobStatus waitForJobToComplete(String jobId) throws TimeoutException, SynapseException, WorkerFailedException {
		long start = clock.currentTimeMillis();
		AsynchronousJobStatus status;
		while (true) {
			long now = clock.currentTimeMillis();
			if (now-start > timeoutMs){
				logger.debug("Timeout waiting for job to complete");
				throw new TimeoutException("Timed out waiting for the job " + jobId + " to complete");
			}
			status = client.getAdminAsynchronousJobStatus(jobId);
			AsynchJobState state = status.getJobState();
			if (state == AsynchJobState.FAILED) {
				logger.debug("Job " + jobId + " failed.");
				throw new WorkerFailedException("Failed:\n" + status.getErrorDetails() + "\n\nmessage:\n" + status.getErrorMessage());
			}
			if (state == AsynchJobState.COMPLETE) {
				break;
			}
			logger.debug("Waiting for job " + request.getClass().getName());
			try {
				clock.sleep(1000L);
			} catch (InterruptedException e) {
				assert false;
			}
		}
		return status;
	}

}
