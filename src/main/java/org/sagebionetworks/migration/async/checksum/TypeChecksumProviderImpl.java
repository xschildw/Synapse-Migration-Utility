package org.sagebionetworks.migration.async.checksum;

import java.util.Iterator;
import java.util.LinkedList;

import org.sagebionetworks.migration.async.DestinationJob;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;

import com.google.common.collect.Iterators;
import com.google.inject.Inject;


public class TypeChecksumProviderImpl implements TypeChecksumBuilder {

	RangeCheksumBuilder rangeProvider;
	int batchSize;

	@Inject
	public TypeChecksumProviderImpl(Configuration configuration, RangeCheksumBuilder rangeProvider) {
		super();
		this.batchSize = configuration.getMaximumBackupBatchSize();
		this.rangeProvider = rangeProvider;
	}

	@Override
	public Iterator<DestinationJob> buildAllRestoreJobsForType(TypeToMigrateMetadata primaryType, String salt) {
		Iterator<DestinationJob> iterator = new LinkedList<DestinationJob>().iterator();
		// divide the full range into batches
		for (long minimumId = primaryType.getSrcMinId(); minimumId < primaryType
				.getSrcMaxId(); minimumId += batchSize) {
			long maximumId = Math.min(primaryType.getSrcMaxId(), minimumId + batchSize);
			Iterator<DestinationJob> range = rangeProvider.providerRangeCheck(primaryType.getType(), minimumId,
					maximumId, salt);
			Iterators.concat(iterator, range);
		}
		return iterator;
	}

}
