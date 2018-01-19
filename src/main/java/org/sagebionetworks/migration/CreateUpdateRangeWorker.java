package org.sagebionetworks.migration;

import java.util.concurrent.Callable;

import org.sagebionetworks.migration.async.AsynchronousJobExecutor;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;
import org.sagebionetworks.repo.model.migration.MigrationType;
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
		// TODO Auto-generated method stub
		return null;
	}

}
