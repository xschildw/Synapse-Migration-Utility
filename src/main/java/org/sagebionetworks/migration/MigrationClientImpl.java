package org.sagebionetworks.migration;

import org.apache.logging.log4j.Logger;
import org.sagebionetworks.migration.config.Configuration;

import com.google.inject.Inject;

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
		stackStatus.setDestinationReadOnly();
		attemptMigraionWithRetry();
		if (! config.remainInReadOnlyAfterMigration()) {
			logger.info("Setting the destination to READ-WRITE mode.");
			stackStatus.setDestinationReadWrite();
		} else {
			logger.info("Destination remains in READ-ONLY mode.");
		}
	}

	/**
	 * Attempt the migration. If there is a failure, retry until the max number of
	 * retries are exhausted.
	 */
	void attemptMigraionWithRetry() {
		for (int tryCount = 0; tryCount < config.getMaxRetries(); tryCount++) {
			try {
				logger.info("Attempting migration try number: " + tryCount + "...");
				fullMigration.runFullMigration();
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
