package org.sagebionetworks.migration;

import java.util.List;

import org.sagebionetworks.repo.model.migration.MigrationType;

public interface AsynchronousMigration {
	
	/**
	 * Migrate the common primary types Asynchronously.
	 * 
	 * @param commonPrimaryTypes
	 */
	void migratePrimaryTypes(List<MigrationType> commonPrimaryTypes);

}
