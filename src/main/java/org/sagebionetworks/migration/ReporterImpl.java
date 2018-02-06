package org.sagebionetworks.migration;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.sagebionetworks.migration.async.JobTarget;
import org.sagebionetworks.migration.async.ResultPair;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.MigrationTypeCountDiff;
import org.sagebionetworks.migration.utils.ToolMigrationUtils;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.AdminRequest;
import org.sagebionetworks.repo.model.migration.AsyncMigrationRequest;
import org.sagebionetworks.repo.model.migration.AsyncMigrationResponse;
import org.sagebionetworks.repo.model.migration.MigrationTypeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;
import org.sagebionetworks.util.Clock;

import com.amazonaws.services.sqs.model.UnsupportedOperationException;
import com.google.inject.Inject;

public class ReporterImpl implements Reporter {

	private static final String ELASE_MS_TEMPLATE = "%02d:%02d:%02d.%03d";
	private static final String WAITING_FOR_JOB_TEMPLATE = "Waiting for jobId %s on %s of type '%s' elapse: %s";
	static final long ONE_SECOND_MS = 1000L;
	static final String COUNTDOWN_FORMAT = "Migration will start in %1$s seconds...";
	static final String STARTING_MIGRATION = "Starting migration...";

	Configuration configuration;
	Logger logger;
	Clock clock;

	@Inject
	public ReporterImpl(Configuration configuration, LoggerFactory loggerFaactory, Clock clock) {
		super();
		this.configuration = configuration;
		this.logger = loggerFaactory.getLogger(ReporterImpl.class);
		this.clock = clock;
	}

	@Override
	public void reportCountDifferences(ResultPair<List<MigrationTypeCount>> typeCounts) {
		List<MigrationTypeCountDiff> diffs = ToolMigrationUtils.getMigrationTypeCountDiffs(typeCounts.getSourceResult(),
				typeCounts.getDestinationResult());
		for (MigrationTypeCountDiff diff : diffs) {
			// Missing at source
			if (diff.getDelta() == null) {
				logger.info("\t" + diff.getType().name() + "\tNA\t" + diff.getDestinationCount());
			} else if (diff.getDelta() != 0L) {
				logger.info("\t" + diff.getType().name() + ":\t" + diff.getDelta() + "\t" + diff.getSourceCount() + "\t"
						+ diff.getDestinationCount());
			}
		}
	}

	@Override
	public void runCountDownBeforeStart() {
		long countDownMS = configuration.getDelayBeforeMigrationStartMS();
		while (countDownMS > 0) {
			logger.info(String.format(COUNTDOWN_FORMAT, TimeUnit.MILLISECONDS.toSeconds(countDownMS)));
			try {
				clock.sleep(ONE_SECOND_MS);
			} catch (InterruptedException e) {
				throw new AsyncMigrationException(e);
			}
			countDownMS -= ONE_SECOND_MS;
		}
		logger.info(STARTING_MIGRATION);
	}

	@Override
	public void reportChecksums(ResultPair<List<MigrationTypeChecksum>> checksums) {
		throw new UnsupportedOperationException("Need to add support");
	}

	@Override
	public void reportProgress(JobTarget jobTarget, AsynchronousJobStatus jobStatus) {
		AsyncMigrationRequest request = (AsyncMigrationRequest) jobStatus.getRequestBody();
		AdminRequest adminRequest = request.getAdminRequest();
		long jobStratedOn = jobStatus.getStartedOn().getTime();
		long elapse = clock.currentTimeMillis() - jobStratedOn;
		String elapseString = formatElapse(elapse);
		logger.info(String.format(WAITING_FOR_JOB_TEMPLATE, jobStatus.getJobId(), jobTarget.name(),
				adminRequest.getClass().getSimpleName(), elapseString));
	}
	
	/**
	 * For the given elapse milliseconds to: 'hh:mm:ss:MMM'
	 * @param elapse
	 * @return
	 */
	public static String formatElapse(long elapse) {
		long hours = TimeUnit.MILLISECONDS.toHours(elapse);
		long mins = TimeUnit.MILLISECONDS.toMinutes(elapse) % 60;
		long sec = TimeUnit.MILLISECONDS.toSeconds(elapse) % 60;
		long ms = TimeUnit.MILLISECONDS.toMillis(elapse) % 1000;
		return String.format(ELASE_MS_TEMPLATE, hours, mins, sec, ms);
	}

}
