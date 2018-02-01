package org.sagebionetworks.migration;

import java.util.List;

import org.sagebionetworks.migration.async.ResultPair;
import org.sagebionetworks.migration.async.JobTarget;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.MigrationTypeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;

public interface Reporter {

	/**
	 * Report the type count differences to the log.
	 * 
	 * @param counts
	 */
	public void reportCountDifferences(ResultPair<List<MigrationTypeCount>> typeCounts);

	/**
	 * Report a count down before the migration process starts.
	 */
	public void runCountDownBeforeStart();

	/**
	 * Report the final checksums for all types.
	 * @param checksums
	 */
	public void reportChecksums(ResultPair<List<MigrationTypeChecksum>> checksums);
	
	/**
	 * Report the progress of an AsynchronousJobStatus.
	 * @param jobTarget Where the job is run.
	 * @param jobStatus Status of the job.
	 */
	public void reportProgress(JobTarget jobTarget, AsynchronousJobStatus jobStatus);
}
