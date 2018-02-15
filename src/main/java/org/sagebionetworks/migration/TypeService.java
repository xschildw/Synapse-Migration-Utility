package org.sagebionetworks.migration;

import java.util.List;

import org.sagebionetworks.migration.async.ResultPair;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;

public interface TypeService {
	
	/**
	 * Get all of the types (primary and secondary) that both the source and destination have in common.
	 * 
	 * @return
	 */
	List<MigrationType> getAllCommonMigrationTypes();

	/**
	 * Get all of the primary types that both the source and destination have in common.
	 * @return
	 */
	List<MigrationType> getCommonPrimaryMigrationTypes();

	/**
	 * Get the counts for the given migration types for both the source and
	 * destination.
	 * 
	 * @param migrationTypes
	 * @return
	 * @throws AsyncMigrationException
	 */
	public ResultPair<List<MigrationTypeCount>> getMigrationTypeCounts(
			List<MigrationType> migrationTypes) throws AsyncMigrationException;

	/**
	 * Get the full table checksum for all of the provided types.
	 * @param migrationTypes
	 * @return
	 */
	public ResultPair<MigrationTypeChecksum> getFullTableChecksums(
			MigrationType migrationType);
}
