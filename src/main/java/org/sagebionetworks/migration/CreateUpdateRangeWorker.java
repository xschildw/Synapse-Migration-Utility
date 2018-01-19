package org.sagebionetworks.migration;

import java.util.concurrent.Callable;

import org.sagebionetworks.migration.async.AsynchronousJobExecutor;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;
import org.sagebionetworks.repo.model.migration.BackupTypeRangeRequest;
import org.sagebionetworks.repo.model.migration.BackupTypeResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.RestoreTypeRequest;
import org.sagebionetworks.repo.model.migration.RestoreTypeResponse;
import org.sagebionetworks.tool.progress.BasicProgress;

public class CreateUpdateRangeWorker implements Callable<Long> {
	
	MigrationType type;
	BackupAliasType aliasType;
	BasicProgress progress;
	AsynchronousJobExecutor	 jobExecutor;
	long minimumId;
	long maximumId;
	long maxBackupBatchSize;

	public CreateUpdateRangeWorker(MigrationType type, BackupAliasType aliasType, BasicProgress progress,
			AsynchronousJobExecutor jobExecutor, long minimumId, long maximumId, long maxBackupBatchSize) {
		super();
		this.type = type;
		this.aliasType = aliasType;
		this.progress = progress;
		this.jobExecutor = jobExecutor;
		this.minimumId = minimumId;
		this.maximumId = maximumId;
		this.maxBackupBatchSize = maxBackupBatchSize;
	}


	@Override
	public Long call() throws Exception {
		progress.setMessage("Starting full backup job");
		
		// execute the backup job on the source.
		BackupTypeRangeRequest backupRequest = new BackupTypeRangeRequest();
		backupRequest.setAliasType(this.aliasType);
		backupRequest.setBatchSize(this.maxBackupBatchSize);
		backupRequest.setMigrationType(this.type);
		backupRequest.setMinimumId(this.minimumId);
		backupRequest.setMaximumId(this.maximumId);
		BackupTypeResponse backupResponse = jobExecutor.executeSourceJob(backupRequest, BackupTypeResponse.class);
		
		progress.setMessage("Starting restore job for "+backupResponse.getBackupFileKey());
		
		// Execute the restore on the destination.
		RestoreTypeRequest restoreRequest = new RestoreTypeRequest();
		restoreRequest.setAliasType(aliasType);
		restoreRequest.setBatchSize(this.maxBackupBatchSize);
		restoreRequest.setMigrationType(this.type);
		restoreRequest.setBackupFileKey(backupResponse.getBackupFileKey());
		RestoreTypeResponse restoreResponse = jobExecutor.executeDestinationJob(restoreRequest, RestoreTypeResponse.class);

		// Update the progress
		progress.setMessage("Finished full restore for "+restoreResponse.getRestoredRowCount()+" rows");
		return restoreResponse.getRestoredRowCount();
	}

}
