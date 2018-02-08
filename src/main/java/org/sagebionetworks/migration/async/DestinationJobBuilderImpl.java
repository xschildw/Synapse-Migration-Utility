package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.List;

import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;

import com.google.common.collect.Iterators;
import com.google.inject.Inject;

public class DestinationJobBuilderImpl implements DestinationJobBuilder {
	
	MissingFromDestinationBuilder missingFromDestinationBuilder;
	
	ChecksumDeltaBuilder checksumChangeBuilder;
	
	@Inject
	public DestinationJobBuilderImpl(MissingFromDestinationBuilder missingFromDestinationBuilder) {
		super();
		this.missingFromDestinationBuilder = missingFromDestinationBuilder;
	}

	@Override
	public Iterator<DestinationJob> buildDestinationJobs(final List<TypeToMigrateMetadata> primaryTypes) {
		// Phase one: Add all rows that are missing from the destination
		Iterator<DestinationJob> missingIterator = missingFromDestinationBuilder.buildDestinationJobs(primaryTypes);
		// Phase two: Find any remaining deltas using checksum-divide-and-conquer 
		Iterator<DestinationJob> checksumIterator = checksumChangeBuilder.buildChecksumJobs(primaryTypes);
		return Iterators.concat(missingIterator, checksumIterator);
	}

}
