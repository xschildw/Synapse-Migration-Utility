package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.List;

import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
/**
 * 
 * Build the iterator that will find deltas using checksum-divide-and-conquer.
 *
 */
public interface ChecksumDeltaBuilder {

	/**
	 * Build the iterator that will find deltas using checksum-divide-and-conquer.
	 * @param primaryTypes
	 * @return
	 */
	Iterator<DestinationJob> buildChecksumJobs(List<TypeToMigrateMetadata> primaryTypes);

}
