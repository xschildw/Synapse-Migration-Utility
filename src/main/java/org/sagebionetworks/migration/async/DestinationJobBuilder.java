package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.List;

import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;

/**
 * Abstraction for generating restore/delete jobs to be run in parallel against the destination server.
 *
 */
public interface DestinationJobBuilder {

	/**
	 * Will build destination jobs on demand.
	 * @param primaryTypes
	 * @return
	 */
	Iterator<DestinationJob> buildDestinationJobs(List<TypeToMigrateMetadata> primaryTypes);
}
