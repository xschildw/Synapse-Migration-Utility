package org.sagebionetworks.migration.async.checksum;

import java.util.Iterator;

import org.sagebionetworks.migration.async.DestinationJob;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;

public interface TypeChecksumBuilder {

	/**
	 * For the given migration type, divide the full ID range into batches.  For each ID batch range,
	 * compare the checksums of the source and destination. For each case where the checksums
	 * do not match, create a backup to be restored on the destination. This builder
	 * provides an iterator over all of the resulting restore jobs.
	 * 
	 * @param primaryType
	 * @param salt
	 * @return
	 */
	Iterator<DestinationJob> buildAllRestoreJobsForType(TypeToMigrateMetadata primaryType, String salt);
}
