package org.sagebionetworks.migration.async.checksum;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.sagebionetworks.migration.async.DestinationJob;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;

import com.google.common.collect.Iterators;
import com.google.inject.Inject;

public class ChecksumDeltaBuilderImpl implements ChecksumDeltaBuilder {

	RangeCheksumBuilder rangeProvider;

	@Inject
	public ChecksumDeltaBuilderImpl(RangeCheksumBuilder rangeProvider) {
		super();
		this.rangeProvider = rangeProvider;
	}

	@Override
	public Iterator<DestinationJob> buildAllRestoreJobsForMismatchedChecksums(
			List<TypeToMigrateMetadata> primaryTypes) {
		// The same salt is used for all types.
		String salt = UUID.randomUUID().toString();
		// Concatenate the iterators for each type.
		Iterator<DestinationJob> iterator = new LinkedList<DestinationJob>().iterator();
		for (TypeToMigrateMetadata primary : primaryTypes) {
			Iterator<DestinationJob> typeIterator = rangeProvider.providerRangeCheck(primary.getType(),
					primary.getSrcMinId(), primary.getSrcMaxId(), salt);
			iterator = Iterators.concat(iterator, typeIterator);
		}
		return iterator;
	}

}
