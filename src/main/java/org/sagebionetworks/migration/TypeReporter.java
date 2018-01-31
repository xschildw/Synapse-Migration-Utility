package org.sagebionetworks.migration;

import java.util.List;

import org.sagebionetworks.migration.async.ConcurrentExecutionResult;
import org.sagebionetworks.repo.model.migration.MigrationTypeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;

public interface TypeReporter {

	/**
	 * Report the type count differences to the log.
	 * 
	 * @param counts
	 */
	public void reportCountDifferences(ConcurrentExecutionResult<List<MigrationTypeCount>> typeCounts);

	/**
	 * Report a count down before the migration process starts.
	 */
	public void runCountDownBeforeStart();

	/**
	 * Report the final checksums for all types.
	 * @param checksums
	 */
	public void reportChecksums(ConcurrentExecutionResult<List<MigrationTypeChecksum>> checksums);
}
