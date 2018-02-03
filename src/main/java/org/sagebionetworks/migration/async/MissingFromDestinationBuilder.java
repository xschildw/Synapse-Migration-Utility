package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.List;

import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;

/**
 * Abstraction for a job builder to add rows from the source that are missing from the destination.
 *
 */
public interface MissingFromDestinationBuilder {

	/**
	 * Will create restore jobs for all rows where the maximum ID of the sources is larger 
	 * than the maximum ID at the destination.
	 * @param primaryTypes
	 * @return
	 */
	Iterator<DestinationJob> buildDestinationJobs(List<TypeToMigrateMetadata> primaryTypes);

}
