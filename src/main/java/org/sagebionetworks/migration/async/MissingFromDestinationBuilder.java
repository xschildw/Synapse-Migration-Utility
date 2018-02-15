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
	 * Will backup all data that is outside the box that is common to both the source and destination
	 * for each type.
	 * @param primaryTypes
	 * @return
	 */
	Iterator<DestinationJob> buildDestinationJobs(List<TypeToMigrateMetadata> primaryTypes);

}
