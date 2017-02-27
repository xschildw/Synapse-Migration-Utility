package org.sagebionetworks.migration.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.factory.AsyncMigrationTypeChecksumWorkerFactory;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeChecksum;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ConcurrentMigrationTypeChecksumsExecutor {
    static private Logger logger = LogManager.getLogger(ConcurrentMigrationTypeCountsExecutor.class);

    private ExecutorService threadPool;
    private AsyncMigrationTypeChecksumWorkerFactory workerFactory;

    public ConcurrentMigrationTypeChecksumsExecutor(ExecutorService executorSvc,
                                                    AsyncMigrationTypeChecksumWorkerFactory workerFactory) {
        this.threadPool = executorSvc;
        this.workerFactory = workerFactory;
    }

    public ConcurrentExecutionResult<String> getMigrationTypeChecksums(MigrationType migrationType, long timeoutMS) throws AsyncMigrationException {

        try {
            AsyncMigrationTypeChecksumWorker sourceWorker = workerFactory.getSourceWorker(migrationType);
            Future<MigrationTypeChecksum> futureSourceMigrationTypeChecksum = threadPool.submit(sourceWorker);
            AsyncMigrationTypeChecksumWorker destinationWorker = workerFactory.getDestinationWorker(migrationType);
            Future<MigrationTypeChecksum> futureDestinationMigrationTypeChecksum = threadPool.submit(destinationWorker);

            // Wait for results
            MigrationTypeChecksum sourceMigrationTypeChecksum = futureSourceMigrationTypeChecksum.get();
            MigrationTypeChecksum destinationMigrationTypeChecksum = futureDestinationMigrationTypeChecksum.get();

            ConcurrentExecutionResult<String> result = new ConcurrentExecutionResult<String>();
            result.setSourceResult(sourceMigrationTypeChecksum.getChecksum());
            result.setDestinationResult(destinationMigrationTypeChecksum.getChecksum());

            return result;
        } catch (InterruptedException e) {
            logger.debug("Caught InterruptedException!");
            throw new AsyncMigrationException("Exception caught in ConcurrentMigrationTypeChecksumsExecutor.", e.getCause());
        }  catch (ExecutionException e) {
            logger.debug("Caught ExecutionException!");
            throw new AsyncMigrationException("Exception caught in ConcurrentMigrationTypeChecksumsExecutor.", e.getCause());
        }
    }
}
