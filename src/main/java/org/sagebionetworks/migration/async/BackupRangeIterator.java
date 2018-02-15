package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.List;

import org.sagebionetworks.repo.model.migration.BackupTypeRangeRequest;
import org.sagebionetworks.repo.model.migration.BackupTypeResponse;

/**
 * Executes the given list of backup requests on demand.
 *
 */
public class BackupRangeIterator implements Iterator<DestinationJob> {

	AsynchronousJobExecutor asynchronousJobExecutor;
	Iterator<BackupTypeRangeRequest> requestIterator;

	public BackupRangeIterator(AsynchronousJobExecutor asynchronousJobExecutor, List<BackupTypeRangeRequest> backupRequests) {
		this.asynchronousJobExecutor = asynchronousJobExecutor;
		this.requestIterator = backupRequests.iterator();
	}

	@Override
	public boolean hasNext() {
		return requestIterator.hasNext();
	}

	@Override
	public DestinationJob next() {
		BackupTypeRangeRequest rangeRequest = requestIterator.next();
		// execute the backup job on the source
		BackupTypeResponse reponse = asynchronousJobExecutor.executeSourceJob(rangeRequest, BackupTypeResponse.class);
		// return the restore job
		return new RestoreDestinationJob(rangeRequest.getMigrationType(), reponse.getBackupFileKey(), rangeRequest.getMinimumId(), rangeRequest.getMaximumId());
	}
}
