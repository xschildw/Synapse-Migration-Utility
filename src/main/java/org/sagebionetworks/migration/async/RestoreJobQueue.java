package org.sagebionetworks.migration.async;

import org.sagebionetworks.migration.AsyncMigrationException;

/**
 * Abstraction for off-loading DestinationJob to be run asynchronously.
 *
 */
public interface RestoreJobQueue {
	
	/**
	 * Push a new job to be executed on the destination.
	 * 
	 * @param job
	 */
	public void pushJob(DestinationJob job);
	
	/**
	 * Are all jobs done?
	 *
	 *@throws If one or more AsyncMigrationException occur while processing the jobs, this method will
	 *throw the last AsyncMigrationException encountered after all jobs are done.
	 * @return
	 */
	public boolean isDone() throws AsyncMigrationException;


}
