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
	 * @return
	 */
	public boolean isDone();

	/**
	 * Get the last observed exception.
	 * @return
	 */
	public AsyncMigrationException getLastException();

}
