package org.sagebionetworks.migration.async;

import java.util.Iterator;

import org.sagebionetworks.repo.model.migration.MigrationType;

/**
 * Abstraction for executing backup jobs.
 *
 */
public interface BackupJobExecutor {

	/**
	 * Typically, a backup job can be executed as a single call. However, for cases
	 * where the number of secondary rows for a given type is high, multiple backup
	 * jobs are needed to to cover a range. Therefore, this method will result in
	 * one or more DestinationJobs.
	 * 
	 * @param type
	 * @param minimumId
	 * @param maximumId
	 * @return
	 */
	public Iterator<DestinationJob> executeBackupJob(MigrationType type, long minimumId, long maximumId);
}
