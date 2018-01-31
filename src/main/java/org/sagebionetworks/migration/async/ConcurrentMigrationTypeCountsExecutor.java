package org.sagebionetworks.migration.async;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.TypeService;
import org.sagebionetworks.migration.factory.AsyncMigrationTypeCountsWorkerFactory;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;
import org.sagebionetworks.repo.model.migration.MigrationTypeCounts;

public class ConcurrentMigrationTypeCountsExecutor {
    static private Logger logger = LogManager.getLogger(ConcurrentMigrationTypeCountsExecutor.class);

    private ExecutorService threadPool;
    private AsyncMigrationTypeCountsWorkerFactory workerFactory;

    public ConcurrentMigrationTypeCountsExecutor(ExecutorService threadPool,
                                                 AsyncMigrationTypeCountsWorkerFactory asyncMigrationTypeCountsFactory) {
        this.threadPool = threadPool;
        this.workerFactory = asyncMigrationTypeCountsFactory;
    }

    public ConcurrentExecutionResult<List<MigrationTypeCount>> getMigrationTypeCounts(List<MigrationType> migrationTypes) throws AsyncMigrationException {

        try {
            AsyncMigrationTypeCountsWorker sourceWorker = this.workerFactory.getSourceWorker(migrationTypes);
            Future<MigrationTypeCounts> futureSourceMigrationTypeCounts = this.threadPool.submit(sourceWorker);
            AsyncMigrationTypeCountsWorker destinationWorker = this.workerFactory.getDestinationWorker(migrationTypes);
            Future<MigrationTypeCounts> futureDestinationMigrationTypeCounts = this.threadPool.submit(destinationWorker);

            // Wait for results
            MigrationTypeCounts sourceMigrationTypeCounts = futureSourceMigrationTypeCounts.get();
            MigrationTypeCounts destinationMigrationTypeCounts = futureDestinationMigrationTypeCounts.get();

            ConcurrentExecutionResult<List<MigrationTypeCount>> results = new ConcurrentExecutionResult<List<MigrationTypeCount>>();
            results.setSourceResult(sourceMigrationTypeCounts.getList());
            results.setDestinationResult(destinationMigrationTypeCounts.getList());
            return results;
        } catch (InterruptedException e) {
            logger.debug("Caught InterruptedException!");
            throw new AsyncMigrationException("Exception caught in ConcurrentMigrationTypeCountsExecutor.", e.getCause());
        }  catch (ExecutionException e) {
            logger.debug("Caught ExecutionException!");
            throw new AsyncMigrationException("Exception caught in ConcurrentMigrationTypeCountsExecutor.", e.getCause());
        }
    }

}
