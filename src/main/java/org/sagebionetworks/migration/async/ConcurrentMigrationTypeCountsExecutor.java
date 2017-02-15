package org.sagebionetworks.migration.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;
import org.sagebionetworks.repo.model.migration.MigrationTypeCounts;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ConcurrentMigrationTypeCountsExecutor {
    static private Logger logger = LogManager.getLogger(ConcurrentMigrationTypeCountsExecutor.class);

    private ExecutorService executorSvc;
    private SynapseClientFactory factory;
    private List<MigrationType> migrationTypes;
    private long timeoutMS;
    private AsyncMigrationTypeCountsWorker sourceWorker;
    private AsyncMigrationTypeCountsWorker destinationWorker;
    private Future<MigrationTypeCounts> futureSourceMigrationTypeCounts;
    private Future<MigrationTypeCounts> futureDestinationMigrationTypeCounts;

    public ConcurrentMigrationTypeCountsExecutor(ExecutorService executorSvc,
                                                 SynapseClientFactory factory,
                                                 List<MigrationType> migrationTypes,
                                                 long timeoutMS) {
        this.executorSvc = executorSvc;
        this.factory = factory;
        this.migrationTypes = migrationTypes;
        this.timeoutMS = timeoutMS;
    }

    public ConcurrentExecutionResult<List<MigrationTypeCount>> getMigrationTypeCounts() throws AsyncMigrationException {

        try {
            this.sourceWorker = new AsyncMigrationTypeCountsWorker(this.factory.getSourceClient(), this.migrationTypes, this.timeoutMS);
            this.futureSourceMigrationTypeCounts = this.executorSvc.submit(this.sourceWorker);
            this.destinationWorker = new AsyncMigrationTypeCountsWorker(this.factory.getDestinationClient(), this.migrationTypes, this.timeoutMS);
            this.futureDestinationMigrationTypeCounts = this.executorSvc.submit(this.destinationWorker);

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
