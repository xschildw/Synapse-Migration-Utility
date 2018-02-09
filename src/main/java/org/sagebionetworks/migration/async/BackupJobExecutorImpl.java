package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.repo.model.migration.BackupTypeRangeRequest;
import org.sagebionetworks.repo.model.migration.BackupTypeResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;

import com.google.inject.Inject;

public class BackupJobExecutorImpl implements BackupJobExecutor {
	
	Configuration configuration;
	AsynchronousJobExecutor asynchronousJobExecutor;
	
	@Inject
	public BackupJobExecutorImpl(Configuration configuration, AsynchronousJobExecutor asynchronousJobExecutor) {
		super();
		this.configuration = configuration;
		this.asynchronousJobExecutor = asynchronousJobExecutor;
	}

	@Override
	public Iterator<DestinationJob> executeBackupJob(MigrationType type, long minimumId, long maximumId) {
		// TODO: Catch job failures to create more jobs.
		BackupTypeRangeRequest request = new BackupTypeRangeRequest();
		request.setAliasType(configuration.getBackupAliasType());
		request.setBatchSize((long) configuration.getMaximumBackupBatchSize());
		request.setMigrationType(type);
		request.setMinimumId(minimumId);
		request.setMaximumId(maximumId);
		
		BackupTypeResponse response = asynchronousJobExecutor.executeSourceJob(request, BackupTypeResponse.class);
		
		DestinationJob job = new RestoreDestinationJob(type, response.getBackupFileKey(), minimumId, maximumId);
		List<DestinationJob> list = new LinkedList<>();
		list.add(job);
		return list.iterator();
	}

}
