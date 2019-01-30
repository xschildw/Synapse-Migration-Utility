package org.sagebionetworks.migration.async.checksum;

import java.util.Iterator;
import java.util.List;

import org.sagebionetworks.migration.async.DestinationJob;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;


public interface ChecksumDeltaBuilder {

	/**
	 * For each migration type, compare the checksums of Id ranges that both the
	 * sources and destination have in common. For each case where the checksums do
	 * not match, create a backup to be restored on the destination. This builder
	 * provides an iterator over all of the resulting restore jobs.
	 * 
	 * @param primaryTypes
	 * @return
	 */
	Iterator<DestinationJob> buildAllRestoreJobsForMismatchedChecksums(List<TypeToMigrateMetadata> primaryTypes);

}
