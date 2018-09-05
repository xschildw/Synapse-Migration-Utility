package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.LinkedList;

import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.util.ValidateArgument;

import com.google.common.collect.Iterators;

/**
 * Will backup all data that is outside the box that is common to both the
 * source and destination for each type.
 * 
 * Note: Each backup is created on the next() call so the caller drives the
 * backup processed.
 */
public class MissingFromDestinationIterator implements Iterator<DestinationJob> {

	BackupJobExecutor backupJobExecutor;

	Configuration config;
	Iterator<DestinationJob> jobIterator;

	MigrationType migrationType;
	long minCommonId;
	long absoluteMinId;
	long maxCommonId;
	long absoluteMaxId;
	long srcMinId;
	long srcMaxId;

	public MissingFromDestinationIterator(Configuration config, BackupJobExecutor backupJobExecutor,
			TypeToMigrateMetadata typeToMigrate) {
		super();
		this.config = config;
		this.backupJobExecutor = backupJobExecutor;
		this.migrationType = typeToMigrate.getType();
		srcMinId = (typeToMigrate.getSrcMinId() != null) ? typeToMigrate.getSrcMinId() : 0L;
		srcMaxId = (typeToMigrate.getSrcMaxId() != null) ? typeToMigrate.getSrcMaxId() : 0L;
		// Destination values are null when the destination is empty
		long desMinId = (typeToMigrate.getDestMinId() != null) ? typeToMigrate.getDestMinId() : 0L;
		long desMaxId = (typeToMigrate.getDestMaxId() != null) ? typeToMigrate.getDestMaxId() : 0L;
		long desCount = (typeToMigrate.getDestCount() != null) ? typeToMigrate.getDestCount() : 0L;

		if (desCount < config.getDestinationRowCountToIgnore()) {
			// treat the destination as empty
			desMaxId = desMinId;
		}
		// Find the ID ranges common to both source and destination.
		minCommonId = Math.max(srcMinId, desMinId);
		maxCommonId = Math.min(srcMaxId, desMaxId);
		// Find the absolute mins and maxs
		absoluteMinId = Math.min(srcMinId, desMinId);
		absoluteMaxId = Math.max(srcMaxId, desMaxId);

	}

	@Override
	public boolean hasNext() {
		if (jobIterator == null) {
			jobIterator = new LinkedList<DestinationJob>().iterator();
			if (maxCommonId <= minCommonId) {
				// no rows common between the source and destination so a full backup of the source is required.
				long minimumId = srcMinId;
				long maximumId = srcMaxId + 1;
				Iterator<DestinationJob> iterator = backupJobExecutor.executeBackupJob(migrationType, minimumId,
						maximumId);
				jobIterator = Iterators.concat(jobIterator, iterator);
			} else {
				if (absoluteMinId < minCommonId) {
					// backup the lower range outside of the common box.
					long minimumId = absoluteMinId;
					long maximumId = minCommonId + 1;
					Iterator<DestinationJob> iterator = backupJobExecutor.executeBackupJob(migrationType, minimumId,
							maximumId);
					jobIterator = Iterators.concat(jobIterator, iterator);
				}
				if (absoluteMaxId > maxCommonId) {
					// backup the upper range outside of the common box.
					long minimumId = maxCommonId;
					long maximumId = absoluteMaxId + 1;
					Iterator<DestinationJob> iterator = backupJobExecutor.executeBackupJob(migrationType, minimumId,
							maximumId);
					jobIterator = Iterators.concat(jobIterator, iterator);
				}
			}

		}
		return jobIterator.hasNext();
	}

	@Override
	public DestinationJob next() {
		return jobIterator.next();
	}

}
