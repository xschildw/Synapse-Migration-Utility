package org.sagebionetworks.migration.async;

import java.util.List;

import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;

public interface MigrationDriver {
	
	/**
	 * Migrate the common primary types Asynchronously.
	 * 
	 * @param commonPrimaryTypes
	 */
	void migratePrimaryTypes(List<TypeToMigrateMetadata> primaryTypes);

}
