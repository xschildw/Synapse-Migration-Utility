package org.sagebionetworks.migration.async;

import java.util.concurrent.Future;

/**
 * Abstraction for starting jobs on the destination.
 */
public interface DestinationJobExecutor {
	
	/**
	 * Start a job on the destination tracked by the returned future.
	 * @param job
	 * @return
	 */
	public Future<?> startDestinationJob(DestinationJob job);

}
