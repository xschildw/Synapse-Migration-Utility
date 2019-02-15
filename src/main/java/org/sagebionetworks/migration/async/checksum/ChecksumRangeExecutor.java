package org.sagebionetworks.migration.async.checksum;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.sagebionetworks.migration.async.AsynchronousJobExecutor;
import org.sagebionetworks.migration.async.BackupJobExecutor;
import org.sagebionetworks.migration.async.DestinationJob;
import org.sagebionetworks.migration.async.ResultPair;
import org.sagebionetworks.repo.model.migration.BatchChecksumRequest;
import org.sagebionetworks.repo.model.migration.BatchChecksumResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.RangeChecksum;

/**
 * This executor will first compare the checksums from both the source and
 * destination for the given ID range. If the checksums do not match, then n
 * number of backup jobs will be started to restore the entire range. If the
 * checksums match, no further work is required.
 * <p>
 * No work is done in the constructor of this object. Checksums will not be
 * executed until the first call to {@link #hasNext()}.
 *
 */
public class ChecksumRangeExecutor implements Iterator<DestinationJob> {

	AsynchronousJobExecutor asynchronousJobExecutor;
	BackupJobExecutor backupJobExecutor;
	Long batchSize;
	MigrationType type;
	Long minimumId;
	Long maximumId;
	String salt;
	Iterator<DestinationJob> lastBackupJobs;
	Iterator<RangeChecksum> mismatchedRanges;

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
			Long batchSize, MigrationType type, Long minimumId, Long maximumId, String salt) {
		super();
		this.asynchronousJobExecutor = asynchronousJobExecutor;
		this.backupJobExecutor = backupJobExecutor;
		this.batchSize = batchSize;
		this.type = type;
		this.minimumId = minimumId;
		this.maximumId = maximumId;
		this.salt = salt;
		// start with an empty iterator.
		lastBackupJobs = new LinkedList<DestinationJob>().iterator();
	}

	@Override
	public boolean hasNext() {
		if (mismatchedRanges == null) {
			/*
			 * This is the first call so find all batches with mismatched checksums.
			 */
			this.mismatchedRanges = findAllMismatchedRanges();
		}
		if (lastBackupJobs.hasNext()) {
			return true;
		} else {
			// find the next set of restore jobs
			if (!mismatchedRanges.hasNext()) {
				// we are done.
				return false;
			}
			// Start n number of backup jobs for the mismatched ID range.
			RangeChecksum misMatchRange = mismatchedRanges.next();
			lastBackupJobs = backupJobExecutor.executeBackupJob(type, misMatchRange.getMinimumId(),
					misMatchRange.getMaximumId());
			return lastBackupJobs.hasNext();
		}
	}

	@Override
	public DestinationJob next() {
		return lastBackupJobs.next();
	}

	/**
	 * Find all checksum ranges that do not match on both the source and
	 * destination.
	 * 
	 * @return
	 */
	Iterator<RangeChecksum> findAllMismatchedRanges() {
		BatchChecksumRequest request = new BatchChecksumRequest();
		request.setMigrationType(this.type);
		request.setBatchSize(this.batchSize);
		request.setMinimumId(this.minimumId);
		request.setMaximumId(this.maximumId);
		request.setSalt(this.salt);
		// get all checksums for this range from both the source and destination.
		ResultPair<BatchChecksumResponse> results = asynchronousJobExecutor.executeSourceAndDestinationJob(request,
				BatchChecksumResponse.class);
		BatchChecksumResponse sourceResult = results.getSourceResult();
		BatchChecksumResponse destinationResult = results.getDestinationResult();
		// Map destination bins to their checksums
		Map<Long, RangeChecksum> destinationBinToRange = new HashMap<>();
		if (destinationResult.getCheksums() != null) {
			for (RangeChecksum range : destinationResult.getCheksums()) {
				destinationBinToRange.put(range.getBinNumber(), range);
			}
		}
		List<RangeChecksum> mismatchRanges = new LinkedList<>();
		if (sourceResult.getCheksums() != null) {
			for (RangeChecksum sourceChecksum : sourceResult.getCheksums()) {
				// Find the matching destination checksum by bin
				RangeChecksum destinationChecksum = destinationBinToRange.get(sourceChecksum.getBinNumber());
				if (destinationChecksum == null || !sourceChecksum.equals(destinationChecksum)) {
					// Checksums do not match
					mismatchRanges.add(sourceChecksum);
				}
			}
		}
		return mismatchRanges.iterator();
	}

}
