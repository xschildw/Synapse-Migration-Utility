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
	BackupJobExecutor backupJobExecutor;

	@Inject
	public MissingFromDestinationBuilderImpl(Configuration config, 	BackupJobExecutor backupJobExecutor) {
		super();
		this.config = config;
		this.backupJobExecutor = backupJobExecutor;
	}

	@Override
	public Iterator<DestinationJob> buildDestinationJobs(List<TypeToMigrateMetadata> primaryTypes) {
		// start with an empty iterator
		Iterator<DestinationJob> iterator = new LinkedList<DestinationJob>().iterator();
		// Concatenate an iterator for each type.
		for(TypeToMigrateMetadata typeToMigrate: primaryTypes) {
			// PLFM-5107: skip type if source count is null
			if (typeToMigrate.getSrcMinId() != null) {
				MissingFromDestinationIterator typeIterator = new MissingFromDestinationIterator(config, backupJobExecutor, typeToMigrate);
				iterator = Iterators.concat(iterator, typeIterator);
			}
		}
		return iterator;
	}

}
