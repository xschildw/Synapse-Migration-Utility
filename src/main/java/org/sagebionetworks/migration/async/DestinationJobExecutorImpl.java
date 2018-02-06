package org.sagebionetworks.migration.async;

import java.util.concurrent.Future;

import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.repo.model.migration.RestoreTypeRequest;
import org.sagebionetworks.repo.model.migration.RestoreTypeResponse;

import com.google.inject.Inject;

public class DestinationJobExecutorImpl implements DestinationJobExecutor {
	
	Configuration config;
	AsynchronousJobExecutor asynchronousJobExecutor;
	
	@Inject
	public DestinationJobExecutorImpl(Configuration config, AsynchronousJobExecutor asynchronousJobExecutor) {
		super();
		this.config = config;
		this.asynchronousJobExecutor = asynchronousJobExecutor;
	}

	@Override
	public Future<?> startDestinationJob(DestinationJob job) {
		if(job instanceof RestoreDestinationJob) {
			// start a restore job.
			RestoreDestinationJob restoreJob = (RestoreDestinationJob) job;
			RestoreTypeRequest restoreRequest = new RestoreTypeRequest();
			restoreRequest.setAliasType(config.getBackupAliasType());
			restoreRequest.setBatchSize((long) config.getMaximumBackupBatchSize());
			restoreRequest.setMigrationType(restoreJob.getMigrationType());
			restoreRequest.setBackupFileKey(restoreJob.getBackupFileKey());
			return asynchronousJobExecutor.startDestionationJob(restoreRequest, RestoreTypeResponse.class);
		}else {
			throw new IllegalArgumentException("Unknown job type: "+job.getClass().getName());
		}
	}

}
