package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.LinkedList;

import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;

/**
 * Creates n number of backups batches for rows in the source that do not exist
 * in the destination. The number of backups required is determined by the
 * maximum batch size.
 * 
 * Note: Each backup is created on the next() call so the caller drives the
 * backup processed.
 */
public class MissingFromDestinationIterator implements Iterator<DestinationJob> {

	BackupJobExecutor backupJobExecutor;

	Configuration config;
	Iterator<DestinationJob> jobIterator;
	TypeToMigrateMetadata typeToMigrate;

	public MissingFromDestinationIterator(Configuration config, BackupJobExecutor backupJobExecutor,
			TypeToMigrateMetadata typeToMigrate) {
		super();
		this.config = config;
		this.backupJobExecutor = backupJobExecutor;
		this.typeToMigrate = typeToMigrate;
	}

	@Override
	public boolean hasNext() {
		if (jobIterator == null) {
			// the first call
			long startId = typeToMigrate.getSrcMinId();
			if (typeToMigrate.getDestMaxId() != null) {
				if (typeToMigrate.getDestCount() != null) {
					if (typeToMigrate.getDestCount() > config.getDestinationRowCountToIgnore()) {
						// there are rows in the destination so start there.
						startId = typeToMigrate.getDestMaxId() + 1;
					}
				}
			}
			// Max is exclusive so the end includes + one.
			long endId = typeToMigrate.getSrcMaxId() + 1;
			jobIterator = backupJobExecutor.executeBackupJob(this.typeToMigrate.getType(), startId, endId);
		}
		return jobIterator.hasNext();
	}

	@Override
	public DestinationJob next() {
		return jobIterator.next();
	}

}
