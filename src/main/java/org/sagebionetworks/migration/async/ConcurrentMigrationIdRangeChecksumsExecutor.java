package org.sagebionetworks.migration.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.factory.AsyncMigrationIdRangeChecksumWorkerFactory;
import org.sagebionetworks.repo.model.migration.MigrationRangeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationType;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ConcurrentMigrationIdRangeChecksumsExecutor {
	static private Logger logger = LogManager.getLogger(ConcurrentMigrationIdRangeChecksumsExecutor.class);

	private ExecutorService threadPool;
	private AsyncMigrationIdRangeChecksumWorkerFactory workerFactory;

	public ConcurrentMigrationIdRangeChecksumsExecutor(ExecutorService threadPool, AsyncMigrationIdRangeChecksumWorkerFactory workerFactory) {
		this.threadPool = threadPool;
		this.workerFactory = workerFactory;
	}

	public ConcurrentExecutionResult<String> getIdRangeChecksums(MigrationType type,
																 String salt,
																 long minId,
																 long maxId,
																 long timeoutMS) {
		try {
			AsyncMigrationIdRangeChecksumWorker sourceWorker = workerFactory.getSourceWorker(type, salt, minId, maxId);
			Future<MigrationRangeChecksum> futureSourceChecksum = threadPool.submit(sourceWorker);
			AsyncMigrationIdRangeChecksumWorker destinationWorker = workerFactory.getDestinationWorker(type, salt, minId, maxId);
			Future<MigrationRangeChecksum> futureDestinationChecksum = threadPool.submit(destinationWorker);

			// Wait for results
			MigrationRangeChecksum sourceChecksum = futureSourceChecksum.get();
			MigrationRangeChecksum destinationChecksum = futureDestinationChecksum.get();

			ConcurrentExecutionResult<String> results = new ConcurrentExecutionResult<String>();
			results.setSourceResult(sourceChecksum.getChecksum());
			results.setDestinationResult(destinationChecksum.getChecksum());

			return results;
		} catch (InterruptedException e) {
			logger.debug("Caught InterruptedException!");
			throw new AsyncMigrationException("Exception caught in ConcurrentMigrationTypeChecksumsExecutor.", e.getCause());
		}  catch (ExecutionException e) {
			logger.debug("Caught ExecutionException!");
			throw new AsyncMigrationException("Exception caught in ConcurrentMigrationTypeChecksumsExecutor.", e.getCause());
		}
	}
}
