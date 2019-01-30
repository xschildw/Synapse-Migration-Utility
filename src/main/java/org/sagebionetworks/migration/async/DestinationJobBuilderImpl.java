package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.List;

import org.sagebionetworks.migration.async.checksum.ChecksumDeltaBuilder;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;

import com.google.common.collect.Iterators;
import com.google.inject.Inject;

public class DestinationJobBuilderImpl implements DestinationJobBuilder {

	MissingFromDestinationBuilder missingFromDestinationBuilder;
	ChecksumDeltaBuilder checksumChangeBuilder;

	@Inject
	public DestinationJobBuilderImpl(MissingFromDestinationBuilder missingFromDestinationBuilder,
			ChecksumDeltaBuilder checksumChangeBuilder) {
		super();
		this.missingFromDestinationBuilder = missingFromDestinationBuilder;
		this.checksumChangeBuilder = checksumChangeBuilder;
	}

	@Override
	public Iterator<DestinationJob> buildDestinationJobs(final List<TypeToMigrateMetadata> primaryTypes) {
		// Phase one: Add all rows that are missing from the destination
		Iterator<DestinationJob> missingIterator = missingFromDestinationBuilder.buildDestinationJobs(primaryTypes);
		// Phase two: Find any remaining deltas by comparing checksums from the source
		// and destination for the common ID ranges.
		Iterator<DestinationJob> checksumIterator = checksumChangeBuilder.buildAllRestoreJobsForMismatchedChecksums(primaryTypes);
		return Iterators.concat(missingIterator, checksumIterator);
	}

}
