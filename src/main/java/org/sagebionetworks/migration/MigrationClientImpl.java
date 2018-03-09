package org.sagebionetworks.migration;

import org.apache.logging.log4j.Logger;
import org.sagebionetworks.migration.config.Configuration;

import com.google.inject.Inject;

import static org.sagebionetworks.migration.utils.ToolMigrationUtils.actualMaximumNumberOfDestinationJobs;

public class MigrationClientImpl implements MigrationClient {

	Logger logger;
	Configuration config;
	StackStatusService stackStatus;
	FullMigration fullMigration;

	@Inject
	public MigrationClientImpl(Configuration config, StackStatusService stackStatus, FullMigration fullMigration, LoggerFactory loggerFactory) {
		super();
		this.logger = loggerFactory.getLogger(MigrationClientImpl.class);
		this.config = config;
		this.stackStatus = stackStatus;
		this.fullMigration = fullMigration;
	}

	@Override
	public void migrate() {
		config.logConfiguration();
		// Start by putting the destination stack in read-only mode.
		stackStatus.setDestinationReadOnly();
		attemptMigrationWithRetry();
		/*
		 * The destination must only be set to READ-WRITE if the last migration run was a
		 * success.
		 */
		stackStatus.setDestinationReadWrite();
	}

	/**
	 * Attempt the migration. If there is a failure, retry until the max number of
	 * retries are exhausted.
	 */
	void attemptMigrationWithRetry() {
		for (int tryCount = 0; tryCount < config.getMaxRetries(); tryCount++) {
			try {
				logger.info("Attempting migration try number: " + tryCount + "...");
				fullMigration.runFullMigration(actualMaximumNumberOfDestinationJobs(tryCount, config));
				logger.info("migration successful");
				return;
			} catch (AsyncMigrationException e) {
				logger.error("Migration Failed:", e);
			}
		}
		// all attempts were exhausted.
		throw new AsyncMigrationException("Migration failed to run to completion without error.");
	}

}
