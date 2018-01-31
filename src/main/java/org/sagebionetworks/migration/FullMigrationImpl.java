package org.sagebionetworks.migration;

import java.util.List;

import org.apache.logging.log4j.Logger;
import org.sagebionetworks.migration.async.ConcurrentExecutionResult;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;

import com.google.inject.Inject;

public class FullMigrationImpl implements FullMigration {

	Logger logger;
	StackStatusService stackStatusService;
	TypeService typeService;
	TypeReporter typeReporter;
	AsynchronousMigration asynchronousMigration;
	Configuration config;

	@Inject
	public FullMigrationImpl(LoggerFactory loggerFactory, StackStatusService stackStatusService,
			TypeService typeService, TypeReporter typeReporter, AsynchronousMigration asynchronousMigration,
			Configuration config) {
		super();
		this.logger = loggerFactory.getLogger(FullMigrationImpl.class);
		this.stackStatusService = stackStatusService;
		this.typeService = typeService;
		this.typeReporter = typeReporter;
		this.asynchronousMigration = asynchronousMigration;
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
		ConcurrentExecutionResult<List<MigrationTypeCount>> countResults = typeService
				.getMigrationTypeCounts(allCommonTypes);

		// print the counts to the log.
		typeReporter.reportCountDifferences(countResults);
		// Give the caller a chance to cancel before migration starts
		typeReporter.runCountDownBeforeStart();

		// run the migration process asynchronously
		logger.info("Starting the asynchronous of all types...");
		asynchronousMigration.migratePrimaryTypes(commonPrimaryTypes);

		// Gather the final counts
		logger.info("Computing final counts...");
		countResults = typeService.getMigrationTypeCounts(allCommonTypes);

		// print the counts to the log.
		logger.info("Final counts after migration:");
		typeReporter.reportCountDifferences(countResults);

		if (config.includeFullTableChecksums()) {
			if (stackStatusService.isSourceReadOnly()) {
				logger.info("Starting full table checksums...");
				ConcurrentExecutionResult<List<MigrationTypeChecksum>> checksums = typeService
						.getFullTableChecksums(allCommonTypes);
				typeReporter.reportChecksums(checksums);
			} else {
				logger.info("Source is not in READ-ONLY so the full table checksum will be skipped");
			}
		}
	}

}
