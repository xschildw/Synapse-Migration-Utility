package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;

import com.google.common.collect.Iterators;
import com.google.inject.Inject;

public class ChecksumDeltaBuilderImpl implements ChecksumDeltaBuilder {
	
	Configuration config;
	AsynchronousJobExecutor asynchronousJobExecutor;
	BackupJobExecutor backupJobExecutor;
	
	@Inject
	public ChecksumDeltaBuilderImpl(Configuration config, AsynchronousJobExecutor asynchronousJobExecutor,
			BackupJobExecutor backupJobExecutor) {
		super();
		this.config = config;
		this.asynchronousJobExecutor = asynchronousJobExecutor;
		this.backupJobExecutor = backupJobExecutor;
	}



	@Override
	public Iterator<DestinationJob> buildChecksumJobs(List<TypeToMigrateMetadata> primaryTypes) {
		// The same salt is used for all types.
		String salt = UUID.randomUUID().toString();
		// Concatenate the iterators for each type.
		Iterator<DestinationJob> iterator = new LinkedList<DestinationJob>().iterator();
		for(TypeToMigrateMetadata primary: primaryTypes) {
			Iterator<DestinationJob> typeIterator = new TypeChecksumDeltaIterator(config, asynchronousJobExecutor,backupJobExecutor, primary, salt);
			iterator = Iterators.concat(iterator, typeIterator);
		}
		return iterator;
	}

}
