package org.sagebionetworks.migration.async;

import java.util.Iterator;

import org.sagebionetworks.repo.model.migration.MigrationType;

/**
 * Abstraction for executing backup jobs.
 *
 */
public interface BackupJobExecutor {

	/**
	 * Create as many backup requests as is needed to backup the given range.
	 * 
	 * @param type
	 * @param minimumId
	 * @param maximumId
	 * @return
	 */
	public Iterator<DestinationJob> executeBackupJob(MigrationType type, long minimumId, long maximumId);
}
