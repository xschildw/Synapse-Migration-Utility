package org.sagebionetworks.migration;

/**
 * Abstraction for the migration client.
 *
 */
public interface MigrationClient {

	/**
	 * Execute the full migration.
	 */
	public void migrate();
}
