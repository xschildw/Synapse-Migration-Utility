package org.sagebionetworks.migration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.migration.async.AsynchronousJobExecutorImpl;
import org.sagebionetworks.migration.async.ConcurrentExecutionResult;
import org.sagebionetworks.migration.async.ConcurrentMigrationIdRangeChecksumsExecutor;
import org.sagebionetworks.migration.async.ConcurrentMigrationTypeCountsExecutor;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.delta.DeltaBuilder;
import org.sagebionetworks.migration.delta.DeltaCounts;
import org.sagebionetworks.migration.delta.DeltaData;
import org.sagebionetworks.migration.delta.DeltaFinder;
import org.sagebionetworks.migration.delta.DeltaRanges;
import org.sagebionetworks.migration.factory.AsyncMigrationIdRangeChecksumWorkerFactory;
import org.sagebionetworks.migration.factory.AsyncMigrationIdRangeChecksumWorkerFactoryImpl;
import org.sagebionetworks.migration.factory.AsyncMigrationTypeCountsWorkerFactory;
import org.sagebionetworks.migration.factory.AsyncMigrationTypeCountsWorkerFactoryImpl;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.migration.stream.BufferedRowMetadataReader;
import org.sagebionetworks.migration.stream.BufferedRowMetadataWriter;
import org.sagebionetworks.migration.utils.MigrationTypeCountDiff;
import org.sagebionetworks.migration.utils.ToolMigrationUtils;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;
import org.sagebionetworks.repo.model.migration.MigrationTypeNames;
import org.sagebionetworks.repo.model.status.StackStatus;
import org.sagebionetworks.repo.model.status.StatusEnum;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.sagebionetworks.tool.progress.BasicProgress;

/**
 * The migration client.
 *
 */
public class MigrationClientOld {
	
	static private Logger logger = LogManager.getLogger(MigrationClientOld.class);

	SynapseClientFactory clientFactory;
	AsyncMigrationTypeCountsWorkerFactory typeCountsWorkerFactory;
	AsyncMigrationIdRangeChecksumWorkerFactory idRangeChecksumWorkerFactory;
	ExecutorService threadPool;
	Configuration config;
	AsynchronousJobExecutorImpl jobExecutor;
	

	/**
	 * New migration client.
	 * @param clientFactory
	 */
	public MigrationClientOld(SynapseClientFactory clientFactory, Configuration config) {
		if(clientFactory == null) throw new IllegalArgumentException("Factory cannot be null");
		this.config = config;
		this.clientFactory = clientFactory;
		this.typeCountsWorkerFactory = new AsyncMigrationTypeCountsWorkerFactoryImpl(clientFactory, config.getWorkerTimeoutMs());
		this.idRangeChecksumWorkerFactory = new AsyncMigrationIdRangeChecksumWorkerFactoryImpl(clientFactory, config.getWorkerTimeoutMs());
		threadPool = Executors.newFixedThreadPool(2);
		this.jobExecutor = new AsynchronousJobExecutorImpl(clientFactory, config);
	}

	public List<MigrationType> getCommonMigrationTypes() throws SynapseException {
		SynapseAdminClient srcClient = clientFactory.getSourceClient();
		SynapseAdminClient destClient = clientFactory.getDestinationClient();

		MigrationTypeNames srcMigrationTypeNames = srcClient.getMigrationTypeNames();
		MigrationTypeNames destMigrationTypeNames = destClient.getMigrationTypeNames();

		return getCommonMigrationTypes(srcMigrationTypeNames, destMigrationTypeNames);
	}

	public List<MigrationType> getCommonPrimaryMigrationTypes() throws SynapseException {
		SynapseAdminClient srcClient = clientFactory.getSourceClient();
		SynapseAdminClient destClient = clientFactory.getDestinationClient();

		MigrationTypeNames srcMigrationTypeNames = srcClient.getPrimaryTypeNames();
		MigrationTypeNames destMigrationTypeNames = destClient.getPrimaryTypeNames();

		return getCommonMigrationTypes(srcMigrationTypeNames, destMigrationTypeNames);
	}

	private List<MigrationType> getCommonMigrationTypes(MigrationTypeNames srcMigrationTypeNames, MigrationTypeNames destMigrationTypeNames) {
		List<String> commonTypeNames = getCommonTypeNames(srcMigrationTypeNames, destMigrationTypeNames);

		List<MigrationType> commonTypes = new LinkedList<MigrationType>();
		for (String t: commonTypeNames) {
			commonTypes.add(MigrationType.valueOf(t));
		}
		return commonTypes;
	}

	private List<String> getCommonTypeNames(MigrationTypeNames srcTypeNames, MigrationTypeNames destTypeNames) {
		List<String> commonNames = new LinkedList<String>();
		for (String typeName: destTypeNames.getList()) {
			// Only keep the destination names that are in the source
			if (srcTypeNames.getList().contains(typeName)) {
				commonNames.add(typeName);
			}
		}
		return commonNames;
	}

	/**
	 *
	 * @param status
	 * @param message
	 * @throws SynapseException
	 * @throws JSONObjectAdapterException
	 */
	public void setDestinationStatus(StatusEnum status, String message) throws SynapseException, JSONObjectAdapterException {
		setStatus(this.clientFactory.getDestinationClient(), status, message);
	}

	/**
	 *
	 * @param status
	 * @param message
	 * @throws SynapseException
	 * @throws JSONObjectAdapterException
	 */
	public void setSourceStatus(StatusEnum status, String message) throws SynapseException, JSONObjectAdapterException {
		setStatus(this.clientFactory.getSourceClient(), status, message);
	}

	/**
	 *
	 * @param client
	 * @param status
	 * @param message
	 * @throws SynapseException
	 * @throws JSONObjectAdapterException
	 */
	private static void setStatus(SynapseAdminClient client, StatusEnum status, String message) throws JSONObjectAdapterException, SynapseException{
		StackStatus destStatus = client.getCurrentStackStatus();
		destStatus.setStatus(status);
		destStatus.setCurrentMessage(message);
		destStatus = client.updateCurrentStackStatus(destStatus);
	}

	/**
	 * Migrate all data from the source to destination.
	 * @throws JSONObjectAdapterException 
	 * @throws SynapseException 
	 * 
	 * @throws Exception 
	 */
	public boolean migrateWithRetry() throws SynapseException, JSONObjectAdapterException {
		boolean failed = false;
		try {
			// First set the destination stack status to down
			setDestinationStatus(StatusEnum.READ_ONLY, "Staging is down for data migration");
			for (int i = 0; i < config.getMaxRetries(); i++) {
				try {
					failed = migrate();

				} catch (Exception e) {
					failed = true;
					logger.error("Failed at attempt: " + i + " with error " + e.getMessage(), e);
				}
				if (! failed) {
					break;
				}
			}
		} finally {
			// After migration is complete, re-enable read/write
			setDestinationStatus(StatusEnum.READ_WRITE, "Synapse is ready for read/write");
		}
		return failed;
	}

	private boolean migrate() throws Exception {
		boolean failed;

		// Determine which types to migrate
		logger.info("Determining types to migrate...");
		List<MigrationType> typesToMigrate = this.getCommonMigrationTypes();
		List<MigrationType> primaryTypesToMigrate = this.getCommonPrimaryMigrationTypes();

		// Get the counts
		logger.info("Computing counts for migrating types...");
		ConcurrentMigrationTypeCountsExecutor typeCountsExecutor = new ConcurrentMigrationTypeCountsExecutor(this.threadPool, this.typeCountsWorkerFactory);

		ConcurrentExecutionResult<List<MigrationTypeCount>> migrationTypeCounts = typeCountsExecutor.getMigrationTypeCounts(typesToMigrate);
		List<MigrationTypeCount> startSourceCounts = migrationTypeCounts.getSourceResult();
		List<MigrationTypeCount> startDestinationCounts = migrationTypeCounts.getDestinationResult();

		// Build the metadata for each type
		List<TypeToMigrateMetadata> typesToMigrateMetadata = ToolMigrationUtils.buildTypeToMigrateMetadata(startSourceCounts, startDestinationCounts, primaryTypesToMigrate);

		// Display starting counts
		logger.info("Starting diffs in counts:");
		printDiffsInCounts(startSourceCounts, startDestinationCounts);

		// Actual migration
		this.migrateTypes(typesToMigrateMetadata);

		// Print the final counts
		migrationTypeCounts = typeCountsExecutor.getMigrationTypeCounts(typesToMigrate);
		List<MigrationTypeCount> endSourceCounts = migrationTypeCounts.getSourceResult();
		List<MigrationTypeCount> endDestCounts = migrationTypeCounts.getDestinationResult();

		logger.info("Ending diffs in  counts:");
		printDiffsInCounts(endSourceCounts, endDestCounts);

		// Exit on 1st success
		failed = false;
		return failed;
	}

	/**
	 * Does the actual migration work.
	 *
	 * @param primaryTypes
	 * @param maxBackupBatchSize
	 * @param minRangeSize
	 * @param timeoutMS
	 * @throws Exception
	 */
	public void migrateTypes(List<TypeToMigrateMetadata> primaryTypes)
			throws Exception {

		// Each migration uses a different salt (same for each type)
		String salt = UUID.randomUUID().toString();
		
		Map<MigrationType, Exception> encounteredExceptions = new HashMap<MigrationType, Exception>();
		boolean failedTypeMigration = false;
		TypeToMigrateMetadata changeMeta = null;
		List<DeltaData> deltaList = new LinkedList<DeltaData>();
		for (TypeToMigrateMetadata tm: primaryTypes) {
			if (! tm.getType().equals(MigrationType.CHANGE)) {
				try {
					migrateType(salt, tm);
				} catch (Exception e) {
					logger.info("Type " + tm.getType().name() + " failed to migrate. Message: " + e.getMessage());
					encounteredExceptions.put(tm.getType(), e);
					failedTypeMigration = true;
				}
			} else {
				changeMeta = tm;
			}
		}
		if (changeMeta == null) {
			throw new IllegalStateException("ChangeMeta should never be null!");
		}
		// Do the CHANGES if no failedTypeMigration
		if (! failedTypeMigration) {
			migrateType(salt, changeMeta);
		} else {
			for (MigrationType t: encounteredExceptions.keySet()) {
				logger.info("Migration type: " + t + ". Exception: " + encounteredExceptions.get(t).getMessage());
			}
			throw new RuntimeException("Migration failed. See list of exceptions.");
		}
	}

	public void migrateType(String salt, TypeToMigrateMetadata metadata) throws Exception {
		/*
		 * When the number of in the destination server for this type are less than the threshold
		 * the entire type is migrated without calculating deltas.
		 * migrated without calculating deltas.
		 */
//		if(useFullBackupAndRestore(metadata.getSrcCount(), metadata.getDestCount(), config.getFullTableMigrationThresholdPercentage())) {
//			// execute a full backup/restore for this type.
//			fullBackupAndRestore(metadata.getType());
//		}else {
			// execute partial backup/restore based on calculated deltas.
			migrateTypeUsingDeltas(salt, metadata);
//		}
	}
	
	/**
	 * Execute a full backup/restore for the given type.
	 * @param type
	 * @param maxBackupBatchSize
	 * @param timeout
	 * @throws Exception
	 */
	private void fullBackupAndRestore(MigrationType type) throws Exception {
		BasicProgress progress = new BasicProgress();
		long minimumId = 0L;
		long maximumId = Long.MAX_VALUE;
		CreateUpdateRangeWorker worker = new CreateUpdateRangeWorker(type, config.getBackupAliasType(), progress, jobExecutor, minimumId,
				maximumId, config.getMaximumBackupBatchSize());
		Future<Long> future = this.threadPool.submit(worker);
		while (!future.isDone()) {
			// Log the progress
			String message = progress.getMessage();
			if (message == null) {
				message = "";
			}
			logger.info("Creating/updating data for type: " + type.name() + " Progress: " + progress.getCurrentStatus()
					+ " " + message);
			Thread.sleep(2000);
		}
		Long counts = future.get();
		logger.info("Finished Creating/updating the following counts for type: " + type.name() + " Counts: " + counts);
	}
	
	/**
	 * Should a full backup/restore be used or a partial based on deltas?
	 * 
	 * @param sourceCount The number of rows for a given type that are in the sources server.
	 * @param destinationCount  The number of rows for a given type that are in the destination server.
	 * @param threshold 
	 * @return
	 */
	public static boolean useFullBackupAndRestore(long sourceCount, long destinationCount, float threshold) {
		if(destinationCount < 1) {
			return true;
		}
		float percentInDestination = destinationCount/sourceCount;
		return percentInDestination < threshold;
	}

	/**
	 * Migrate the given type by calculating deltas between the source and destination.
	 * @param salt
	 * @param metadata
	 * @param maxBackupBatchSize
	 * @param minRangeSize
	 * @param timeoutMS
	 * @throws Exception
	 */
	private void migrateTypeUsingDeltas(String salt, TypeToMigrateMetadata metadata) throws Exception {
		// Create the files containing the backup/restore metadata.
		DeltaData dd = calculateDeltaForType(metadata, salt);
		try {
			// deletes
			long delCount =  dd.getCounts().getDelete();
			if (delCount > 0) {
				deleteFromDestination(dd.getType(), delCount, dd.getDeleteTemp());
			}
			// inserts
			long insCount = dd.getCounts().getCreate();
			if (insCount > 0) {
				createUpdateInDestination(dd.getType(), insCount, dd.getCreateTemp());
			}
			// updates
			long updCount = dd.getCounts().getUpdate();
			if (updCount > 0) {
				createUpdateInDestination(dd.getType(), updCount, dd.getUpdateTemp());
			}
		}finally {
			// Cleanup the temp files
			if(dd != null) {
				if(dd.getDeleteTemp() != null) {
					dd.getDeleteTemp().delete();
				}
				if(dd.getUpdateTemp() != null) {
					dd.getUpdateTemp().delete();
				}
				if(dd.getCreateTemp() != null) {
					dd.getCreateTemp().delete();
				}
			}
		}
	}

	/**
	 * Create or update
	 *
	 * @param type
	 * @param createUpdateTemp
	 * @param count
	 * @param maxBackupBatchSize
	 * @param timeout
	 * @throws Exception
	 */
	private void createUpdateInDestination(MigrationType type, long count, File createUpdateTemp) throws Exception {
		BufferedRowMetadataReader reader = new BufferedRowMetadataReader(new FileReader(createUpdateTemp));
		try{
			BasicProgress progress = new BasicProgress();
			CreateUpdateWorker worker = new CreateUpdateWorker(type, count, config.getBackupAliasType(), reader,progress, jobExecutor, config.getMaximumBackupBatchSize());
			Future<Long> future = this.threadPool.submit(worker);
			while(!future.isDone()){
				// Log the progress
				String message = progress.getMessage();
				if(message == null){
					message = "";
				}
				logger.info("Creating/updating data for type: "+type.name()+" Progress: "+progress.getCurrentStatus()+" "+message);
				Thread.sleep(2000);
			}
				Long counts = future.get();
				logger.info("Creating/updating the following counts for type: "+type.name()+" Counts: "+counts);
		} finally {
			reader.close();
		}
	}

	private void printDiffsInCounts(List<MigrationTypeCount> srcCounts, List<MigrationTypeCount> destCounts) {
		List<MigrationTypeCountDiff> diffs = ToolMigrationUtils.getMigrationTypeCountDiffs(srcCounts, destCounts);
		for (MigrationTypeCountDiff d: diffs) {
			// Missing at source
			if (d.getDelta() == null) {
				logger.info("\t" + d.getType().name() + "\tNA\t" + d.getDestinationCount());
			} else if (d.getDelta() != 0L) {
					logger.info("\t" + d.getType().name() + ":\t" + d.getDelta() + "\t" + d.getSourceCount() + "\t" + d.getDestinationCount());
			}
		}
	}

	/**
	 * Calculate deltas for one type
	 *
	 * @param tm
	 * @param salt
	 * @param minRangeSize
	 * @return
	 * @throws Exception
	 */
	public DeltaData calculateDeltaForType(TypeToMigrateMetadata tm, String salt) throws Exception {

		ConcurrentMigrationIdRangeChecksumsExecutor idRangeChecksumsExecutor = new ConcurrentMigrationIdRangeChecksumsExecutor(this.threadPool, this.idRangeChecksumWorkerFactory);
		// First, we find the delta ranges
		DeltaFinder finder = new DeltaFinder(tm, salt, (long) config.getMinimumDeltaRangeSize(), idRangeChecksumsExecutor);
		DeltaRanges ranges = finder.findDeltaRanges();
		
		// the first thing we need to do is calculate the what needs to be created, updated, or deleted.
		// We need three temp file to keep track of the deltas
		File createTemp = File.createTempFile("create", ".tmp");
		File updateTemp = File.createTempFile("update", ".tmp");
		File deleteTemp = File.createTempFile("delete", ".tmp");
		
		// Calculate the deltas
		DeltaCounts counts = calculateDeltas(tm, ranges, createTemp, updateTemp, deleteTemp);
		return new DeltaData(tm.getType(), createTemp, updateTemp, deleteTemp, counts);
		
	}

	/**
	 * Calculate the deltas
	 * @param typeMeta
	 * @param minRangeSize
	 * @param createTemp
	 * @param updateTemp
	 * @param deleteTemp
	 * @throws SynapseException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private DeltaCounts calculateDeltas(TypeToMigrateMetadata typeMeta, DeltaRanges ranges, File createTemp, File updateTemp, File deleteTemp)	throws Exception {

		BasicProgress sourceProgress = new BasicProgress();
		BasicProgress destProgress = new BasicProgress();
		BufferedRowMetadataWriter createOut = null;
		BufferedRowMetadataWriter updateOut = null;
		BufferedRowMetadataWriter deleteOut = null;
		try{
			createOut = new BufferedRowMetadataWriter(new FileWriter(createTemp));
			updateOut = new BufferedRowMetadataWriter(new FileWriter(updateTemp));
			deleteOut = new BufferedRowMetadataWriter(new FileWriter(deleteTemp));
			
			DeltaBuilder builder = new DeltaBuilder(clientFactory, config.getMinimumDeltaRangeSize(), typeMeta, ranges, createOut, updateOut, deleteOut);
			
			// Unconditional inserts
			long insCount = builder.addInsertsFromSource();
			
			// Unconditional deletes
			long delCount = builder.addDeletesAtDestination();;
			
			// Deep comparison of update box
			DeltaCounts counts = builder.addDifferencesBetweenSourceAndDestination();
			counts.setCreate(counts.getCreate()+insCount);
			counts.setDelete(counts.getDelete()+delCount);
			
			logger.info("Calculated the following counts for type: "+typeMeta.getType().name()+" Counts: "+counts);
			
			return counts;
			
		} finally {
			
			if (createOut != null)  {
				try {
					createOut.close();
				} catch (Exception e) {}
			}
			if (updateOut != null) {
				try {
					updateOut.close();
				} catch (Exception e) {}
			}
			if (deleteOut != null) {
				try {
					deleteOut.close();
				} catch (Exception e) {}
			}
		}
	}
	
	/**
	 * Delete the requested object from the destination.
	 * @throws IOException 
	 * 
	 */
	private void deleteFromDestination(MigrationType type, long count, File deleteTemp) throws Exception {
		BufferedRowMetadataReader reader = new BufferedRowMetadataReader(new FileReader(deleteTemp));
		try{
			BasicProgress progress = new BasicProgress();
			DeleteWorker worker = new DeleteWorker(type, count, reader, progress, this.jobExecutor, config.getMaximumBackupBatchSize());
			Future<Long> future = this.threadPool.submit(worker);
			while(!future.isDone()){
				// Log the progress
				logger.info("Deleting data for type: "+type.name()+" Progress: "+progress.getCurrentStatus());
				Thread.sleep(2000);
			}
			Long counts = future.get();
			logger.info("Deleted the following counts for type: "+type.name()+" Counts: "+counts);
		} finally{
			reader.close();
		}

	}
}

