package org.sagebionetworks.migration.async.checksum;

import java.util.Iterator;

import org.sagebionetworks.migration.async.AsynchronousJobExecutor;
import org.sagebionetworks.migration.async.BackupJobExecutor;
import org.sagebionetworks.migration.async.DestinationJob;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.repo.model.migration.MigrationType;

import com.google.inject.Inject;

public class RangeCheksumBuilderImpl implements RangeCheksumBuilder {
	
	AsynchronousJobExecutor asynchronousJobExecutor;
	BackupJobExecutor backupJobExecutor;
	long batchSize;
	
	@Inject
	public RangeCheksumBuilderImpl(AsynchronousJobExecutor asynchronousJobExecutor,
			BackupJobExecutor backupJobExecutor, Configuration config) {
		super();
		this.asynchronousJobExecutor = asynchronousJobExecutor;
		this.backupJobExecutor = backupJobExecutor;
		this.batchSize = config.getMaximumBackupBatchSize();
	}


	@Override
	public Iterator<DestinationJob> providerRangeCheck(MigrationType type, Long minimumId, Long maximumId,
			String salt) {
		return new ChecksumRangeExecutor(asynchronousJobExecutor, backupJobExecutor, batchSize, type, minimumId, maximumId, salt);
	}

}
