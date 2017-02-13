package org.sagebionetworks.migration.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;
import org.sagebionetworks.repo.model.migration.MigrationTypeCounts;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ConcurrentMigrationTypeCountsExecutor {
    static private Logger logger = LogManager.getLogger(ConcurrentMigrationTypeCountsExecutor.class);

    private ExecutorService threadPool;
    private AsyncMigrationTypeCountsWorker sourceWorker;
    private AsyncMigrationTypeCountsWorker destinationWorker;

    public ConcurrentMigrationTypeCountsExecutor(ExecutorService threadPool, AsyncMigrationTypeCountsWorker srcWorker, AsyncMigrationTypeCountsWorker destWorker) {
        this.threadPool = threadPool;
        this.sourceWorker = srcWorker;
        this.destinationWorker = destWorker;
    }

    public ConcurrentExecutionResult<MigrationTypeCounts> getMigrationTypeCounts() {
        ConcurrentExecutionResult<MigrationTypeCounts> results = new ConcurrentExecutionResult<MigrationTypeCounts>();
        Future<MigrationTypeCounts> futureSourceMigrationTypeCounts = this.threadPool.submit(this.sourceWorker);
        Future<MigrationTypeCounts> futureDestinationMigrationTypeCounts = this.threadPool.submit(this.destinationWorker);
        MigrationTypeCounts sourceMigrationTypeCounts = futureSourceMigrationTypeCounts.get();
        MigrationTypeCounts destinationMigrationTypeCounts = futureDestinationMigrationTypeCounts.get();
        results.setSourceResponse(sourceMigrationTypeCounts);
        results.setDestinationResponse(destinationMigrationTypeCounts);
        return results;
    }

    private void waitForResults() {

    }

}
