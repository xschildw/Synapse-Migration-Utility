package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.List;

import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;

import com.google.inject.Inject;

public class DestinationJobBuilderImpl implements DestinationJobBuilder {
	
	MissingFromDestinationBuilder missingFromDestinationBuilder;
	
	@Inject
	public DestinationJobBuilderImpl(MissingFromDestinationBuilder missingFromDestinationBuilder) {
		super();
		this.missingFromDestinationBuilder = missingFromDestinationBuilder;
	}

	@Override
	public Iterator<DestinationJob> buildDestinationJobs(final List<TypeToMigrateMetadata> primaryTypes) {
		// This will be expanded to do other job types.
		return missingFromDestinationBuilder.buildDestinationJobs(primaryTypes);
	}

}
