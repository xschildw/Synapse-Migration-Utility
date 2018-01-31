package org.sagebionetworks.migration;

public interface FullMigration {

	/**
	 * Run a full migration from beginning to end.
	 * 
	 * @throws AsyncMigrationException
	 *             An AsyncMigrationException must be thrown if there were any
	 *             migration failures during the run. This signals that another
	 *             migration attempt is necessary.
	 *             Any other exception type will terminate migration.
	 */
	public void runFullMigration() throws AsyncMigrationException;
}
