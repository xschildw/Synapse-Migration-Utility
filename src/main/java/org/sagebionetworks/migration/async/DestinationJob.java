package org.sagebionetworks.migration.async;

import org.sagebionetworks.repo.model.migration.MigrationType;

/**
 * Abstraction for a job to be run on the destination.
 *
 */
public interface DestinationJob {

	/**
	 * The 
	 * @return
	 */
	public MigrationType getMigrationType();

}
