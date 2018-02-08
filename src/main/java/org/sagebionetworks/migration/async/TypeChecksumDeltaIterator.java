package org.sagebionetworks.migration.async;

import java.util.Iterator;

import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.repo.model.migration.AsyncMigrationRangeChecksumRequest;
import org.sagebionetworks.repo.model.migration.MigrationRangeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationType;

import com.google.common.collect.Iterators;

public class TypeChecksumDeltaIterator implements Iterator<DestinationJob> {

	AsynchronousJobExecutor asynchronousJobExecutor;
	BackupJobExecutor backupJobExecutor;
	int batchSize;
	MigrationType type;
	long minimumId;
	long maximumId;
	String salt;
	Iterator<DestinationJob> nextLevel;

	/**
	 * Start the full range for this type.
	 * 
	 * @param configuration
	 * @param asynchronousJobExecutor
	 * @param backupJobExecutor
	 * @param primaryType
	 * @param salt
	 */
	public TypeChecksumDeltaIterator(Configuration configuration, AsynchronousJobExecutor asynchronousJobExecutor,
			BackupJobExecutor backupJobExecutor, TypeToMigrateMetadata primaryType, String salt) {
		this(configuration.getMaximumBackupBatchSize(), asynchronousJobExecutor, backupJobExecutor,
				primaryType.getType(), primaryType.getSrcMinId(), primaryType.getSrcMaxId(), salt);
	}

	/**
	 * 
	 * @param configuration
	 * @param asynchronousJobExecutor
	 * @param backupJobExecutor
	 * @param type
	 * @param minimumId
	 * @param maximumId
	 * @param salt
	 */
	public TypeChecksumDeltaIterator(int batchSize, AsynchronousJobExecutor asynchronousJobExecutor,
			BackupJobExecutor backupJobExecutor, MigrationType type, long minimumId, long maximumId, String salt) {
		super();
		this.batchSize = batchSize;
		this.asynchronousJobExecutor = asynchronousJobExecutor;
		this.backupJobExecutor = backupJobExecutor;
		this.type = type;
		this.minimumId = minimumId;
		this.maximumId = maximumId;
		this.salt = salt;
	}

	@Override
	public boolean hasNext() {
		// If nextLevel exists we have already checked this level.
		if (nextLevel != null) {
			return nextLevel.hasNext();
		} else {
			// run the checksum on both the source and destination
			ResultPair<MigrationRangeChecksum> result = executeChecksumRequests();
			if (result.getSourceResult().getChecksum().equals(result.getDestinationResult().getChecksum())) {
				// checksum matches
				return false;
			} else {
				// if the range size is less than batch size then run the backup
				long rangeSize = this.maximumId - this.minimumId;
				if (rangeSize < this.batchSize) {
					// backup this range with as many jobs as needed.
					this.nextLevel = backupJobExecutor.executeBackupJob(type, minimumId, maximumId + 1);
				} else {
					// divide and conquer
					this.nextLevel = divideAndConquer();
				}
			}
			// next level is now in control
			return this.nextLevel.hasNext();
		}
	}

	@Override
	public DestinationJob next() {
		return nextLevel.next();
	}

	/**
	 * Execute the checksum requests for this range.
	 * 
	 * @return
	 */
	ResultPair<MigrationRangeChecksum> executeChecksumRequests() {
		// Get the check sum
		AsyncMigrationRangeChecksumRequest request = new AsyncMigrationRangeChecksumRequest();
		// Max is inclusive in checksums
		request.setMaxId(this.maximumId);
		request.setMinId(this.minimumId);
		request.setSalt(salt);
		request.setType(type.name());
		// run the checksum on both the source and destination
		return asynchronousJobExecutor.executeSourceAndDestinationJob(request, MigrationRangeChecksum.class);
	}

	/**
	 * Divide this range into two sub-range checks.
	 * 
	 * @return
	 */
	Iterator<DestinationJob> divideAndConquer() {
		long range = this.maximumId - this.minimumId;
		// divide and conquer
		long middleId = this.minimumId + (range / 2);
		// left
		long leftMinimum = this.minimumId;
		long leftMaximum = middleId;
		Iterator<DestinationJob> left = new TypeChecksumDeltaIterator(batchSize, asynchronousJobExecutor,
				backupJobExecutor, type, leftMinimum, leftMaximum, salt);
		// right
		long rightMinimum = middleId + 1;
		long rightMaximum = this.maximumId;
		Iterator<DestinationJob> right = new TypeChecksumDeltaIterator(batchSize, asynchronousJobExecutor,
				backupJobExecutor, type, rightMinimum, rightMaximum, salt);
		return Iterators.concat(left, right);
	}

}
