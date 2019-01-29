package org.sagebionetworks.migration.async.checksum;

import java.util.Iterator;

import org.sagebionetworks.migration.async.AsynchronousJobExecutor;
import org.sagebionetworks.migration.async.BackupJobExecutor;
import org.sagebionetworks.migration.async.DestinationJob;
import org.sagebionetworks.migration.async.ResultPair;
import org.sagebionetworks.repo.model.migration.AsyncMigrationRangeChecksumRequest;
import org.sagebionetworks.repo.model.migration.MigrationRangeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationType;

/**
 * This executor will first compare the checksums from both the source and
 * destination for the given ID range. If the checksums do not match, then n
 * number of backup jobs will be started to restore the entire range. If the checksums
 * match, no further work is required.
 * <p>
 * No work is done in the constructor of this object. Checksums will not be
 * executed until the first call to {@link #hasNext()}.
 *
 */
public class ChecksumRangeExecutor implements Iterator<DestinationJob> {

	AsynchronousJobExecutor asynchronousJobExecutor;
	BackupJobExecutor backupJobExecutor;
	MigrationType type;
	Long minimumId;
	Long maximumId;
	String salt;
	Iterator<DestinationJob> restoreJobs;

	/**
	 * No work is done in the constructor of this object. Checksums will not be
	 * executed until the first call to {@link #hasNext()}.
	 * 
	 * @param asynchronousJobExecutor
	 * @param backupJobExecutor
	 * @param type
	 * @param minimumId
	 * @param maximumId
	 * @param salt
	 */
	public ChecksumRangeExecutor(AsynchronousJobExecutor asynchronousJobExecutor, BackupJobExecutor backupJobExecutor,
			MigrationType type, Long minimumId, Long maximumId, String salt) {
		super();
		this.asynchronousJobExecutor = asynchronousJobExecutor;
		this.backupJobExecutor = backupJobExecutor;
		this.type = type;
		this.minimumId = minimumId;
		this.maximumId = maximumId;
		this.salt = salt;
	}

	@Override
	public boolean hasNext() {
		if (restoreJobs != null) {
			return restoreJobs.hasNext();
		}
		// the first call
		if (doChecksumsMatch()) {
			// Checksums match so there is nothing more to do.
			return false;
		} else {
			// Checksums do not match so start the backup jobs for this range.
			restoreJobs = backupJobExecutor.executeBackupJob(type, minimumId, maximumId + 1);
			return restoreJobs.hasNext();
		}
	}

	@Override
	public DestinationJob next() {
		if (restoreJobs == null) {
			return null;
		} else {
			return restoreJobs.next();
		}
	}

	/**
	 * Execute the checksum requests for this range.
	 * 
	 * @return
	 */
	boolean doChecksumsMatch() {
		// Get the check sum
		AsyncMigrationRangeChecksumRequest request = new AsyncMigrationRangeChecksumRequest();
		// Max is inclusive in checksums
		request.setMaxId(this.maximumId);
		request.setMinId(this.minimumId);
		request.setSalt(salt);
		request.setMigrationType(type);
		// run the checksum on both the source and destination
		ResultPair<MigrationRangeChecksum> results = asynchronousJobExecutor.executeSourceAndDestinationJob(request,
				MigrationRangeChecksum.class);
		return doChecksumsMatch(results);
	}

	/**
	 * Given the results of the checksums for both the source and destination, do
	 * the checksums match?
	 * 
	 * @param results
	 * @return
	 */
	public static boolean doChecksumsMatch(ResultPair<MigrationRangeChecksum> results) {
		if (results != null) {
			MigrationRangeChecksum sourceResults = results.getSourceResult();
			MigrationRangeChecksum destResults = results.getDestinationResult();
			if (sourceResults != null && destResults != null) {
				String sourceChecksum = sourceResults.getChecksum();
				String destChecksum = destResults.getChecksum();
				// null at both the source and destination means no data for that range.
				if (sourceChecksum == null && destChecksum == null) {
					return true;
				}
				if (sourceChecksum != null && destChecksum != null) {
					return sourceChecksum.equals(destChecksum);
				}
			}
		}
		return false;
	}

}
