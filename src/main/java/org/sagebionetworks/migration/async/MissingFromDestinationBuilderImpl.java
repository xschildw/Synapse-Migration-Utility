package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;

import com.google.common.collect.Iterators;
import com.google.inject.Inject;

public class MissingFromDestinationBuilderImpl implements MissingFromDestinationBuilder {
	
	Configuration config;
	AsynchronousJobExecutor asynchronousJobExecutor;

	@Inject
	public MissingFromDestinationBuilderImpl(Configuration config, AsynchronousJobExecutor asynchronousJobExecutor) {
		super();
		this.config = config;
		this.asynchronousJobExecutor = asynchronousJobExecutor;
	}

	@Override
	public Iterator<DestinationJob> buildDestinationJobs(List<TypeToMigrateMetadata> primaryTypes) {
		// start with an empty iterator
		Iterator<DestinationJob> iterator = new LinkedList<DestinationJob>().iterator();
		// Concatenate an iterator for each type.
		for(TypeToMigrateMetadata typeToMigrate: primaryTypes) {
			MissingFromDestinationIterator typeIterator = new MissingFromDestinationIterator(config, asynchronousJobExecutor, typeToMigrate);
			iterator = Iterators.concat(iterator, typeIterator);
		}
		return iterator;
	}

}
