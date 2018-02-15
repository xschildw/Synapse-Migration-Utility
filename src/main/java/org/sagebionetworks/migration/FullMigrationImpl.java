package org.sagebionetworks.migration;

import java.util.List;

import org.apache.logging.log4j.Logger;
import org.sagebionetworks.migration.async.MigrationDriver;
import org.sagebionetworks.migration.async.ResultPair;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.ToolMigrationUtils;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;

import com.google.inject.Inject;

public class FullMigrationImpl implements FullMigration {

	Logger logger;
	StackStatusService stackStatusService;
	TypeService typeService;
	Reporter typeReporter;
	MigrationDriver migrationDriver;
	Configuration config;

	@Inject
	public FullMigrationImpl(LoggerFactory loggerFactory, StackStatusService stackStatusService,
			TypeService typeService, Reporter typeReporter, MigrationDriver migrationDriver,
			Configuration config) {
		super();
		this.logger = loggerFactory.getLogger(FullMigrationImpl.class);
		this.stackStatusService = stackStatusService;
		this.typeService = typeService;
		this.typeReporter = typeReporter;
		this.migrationDriver = migrationDriver;
		this.config = config;
	}

	@Override
	public void runFullMigration() throws AsyncMigrationException {

		// Start by finding the types both the source and destination have in common.
		logger.info("Determining types to migrate...");
		List<MigrationType> allCommonTypes = typeService.getAllCommonMigrationTypes();
		List<MigrationType> commonPrimaryTypes = typeService.getCommonPrimaryMigrationTypes();

		// Get the counts for all types
		logger.info("Computing counts for migrating types...");
		ResultPair<List<MigrationTypeCount>> countResults = typeService.getMigrationTypeCounts(allCommonTypes);

		// print the counts to the log.
		typeReporter.reportCountDifferences(countResults);
		// Give the caller a chance to cancel before migration starts
		typeReporter.runCountDownBeforeStart();

		// Build the metadata for each type
		List<TypeToMigrateMetadata> typesToMigrate = ToolMigrationUtils.buildTypeToMigrateMetadata(
				countResults.getSourceResult(), countResults.getDestinationResult(), commonPrimaryTypes);
		// run the migration process asynchronously
		logger.info("Starting the asynchronous of all types...");
		migrationDriver.migratePrimaryTypes(typesToMigrate);

		// Gather the final counts
		logger.info("Computing final counts...");
		countResults = typeService.getMigrationTypeCounts(allCommonTypes);

		// print the counts to the log.
		logger.info("Final counts after migration:");
		typeReporter.reportCountDifferences(countResults);

		if (config.includeFullTableChecksums()) {
			if (stackStatusService.isSourceReadOnly()) {
				logger.info("Starting full table checksums...");
				for(MigrationType type: allCommonTypes) {
					ResultPair<MigrationTypeChecksum> checksum = typeService.getFullTableChecksums(type);
					typeReporter.reportChecksums(checksum);
				}
			} else {
				logger.info("Source is not in READ-ONLY so the full table checksum will be skipped");
			}
		}
	}

}
