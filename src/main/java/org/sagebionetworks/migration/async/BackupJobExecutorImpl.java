package org.sagebionetworks.migration.async;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;
import org.sagebionetworks.repo.model.migration.BackupTypeRangeRequest;
import org.sagebionetworks.repo.model.migration.CalculateOptimalRangeRequest;
import org.sagebionetworks.repo.model.migration.CalculateOptimalRangeResponse;
import org.sagebionetworks.repo.model.migration.IdRange;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.util.ValidateArgument;

import com.google.inject.Inject;

public class BackupJobExecutorImpl implements BackupJobExecutor {

	Configuration configuration;
	AsynchronousJobExecutor asynchronousJobExecutor;

	@Inject
	public BackupJobExecutorImpl(Configuration configuration, AsynchronousJobExecutor asynchronousJobExecutor) {
		super();
		this.configuration = configuration;
		this.asynchronousJobExecutor = asynchronousJobExecutor;
	}

	@Override
	public Iterator<DestinationJob> executeBackupJob(MigrationType type, long minimumId, long maximumId) {
		// Request the optimal ranges for this range from the source.
		CalculateOptimalRangeRequest rangeRequest = new CalculateOptimalRangeRequest();
		rangeRequest.setMigrationType(type);
		rangeRequest.setMinimumId(minimumId);
		rangeRequest.setMaximumId(maximumId);
		rangeRequest.setOptimalRowsPerRange((long) configuration.getMaximumBackupBatchSize());
		CalculateOptimalRangeResponse rangeResponse = asynchronousJobExecutor.executeSourceJob(rangeRequest,
				CalculateOptimalRangeResponse.class);
		// Create contiguous backup requests based on the optimal ranges.
		List<BackupTypeRangeRequest> requests = createContiguousBackupRangeRequests(configuration.getBackupAliasType(),
				configuration.getMaximumBackupBatchSize(), type, minimumId, maximumId, rangeResponse.getRanges());
		return new BackupRangeIterator(asynchronousJobExecutor, requests);
	}

	/**
	 * Given a list of sparse ID ranges create a list of contiguous ranges that start with the given minimum
	 * and end with the given maximum 
	 * 
	 * @param minimumId
	 * @param maximumId
	 * @param sparseRange
	 * @return
	 */
	public static List<BackupTypeRangeRequest> createContiguousBackupRangeRequests(final BackupAliasType backupAlisType,
			final long batchSize, final MigrationType migrationType, final long minimumId, final long maximumId,
			List<IdRange> sparseRange) {
		ValidateArgument.required(sparseRange, "sparseRange");
		ValidateArgument.required(!sparseRange.isEmpty(), "sparseRange is empty");
		List<BackupTypeRangeRequest> backupRequests = new LinkedList<>();
		BackupTypeRangeRequest last = null;
		// input minimum is start of the local minimum
		long localMin = minimumId;
		for (IdRange range : sparseRange) {
			BackupTypeRangeRequest request = new BackupTypeRangeRequest();
			request.setAliasType(backupAlisType);
			request.setBatchSize(batchSize);
			request.setMigrationType(migrationType);
			// min of the mins to convert from sparse to contiguous.
			request.setMinimumId(Math.min(localMin, range.getMinimumId()));
			request.setMaximumId(range.getMaximumId());
			backupRequests.add(request);
			// setup next
			localMin = request.getMaximumId()+1;
			last = request;
		}
		// The last request must use the provided maximum ID
		last.setMaximumId(maximumId);
		return backupRequests;
	}

}
