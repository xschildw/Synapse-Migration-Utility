package org.sagebionetworks.migration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.migration.async.AsyncMigrationWorker;
import org.sagebionetworks.migration.async.ConcurrentExecutionResult;
import org.sagebionetworks.migration.async.ConcurrentMigrationTypeCountsExecutor;
import org.sagebionetworks.migration.delta.*;
import org.sagebionetworks.repo.model.migration.*;
import org.sagebionetworks.repo.model.status.StackStatus;
import org.sagebionetworks.repo.model.status.StatusEnum;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.migration.stream.BufferedRowMetadataReader;
import org.sagebionetworks.migration.stream.BufferedRowMetadataWriter;
import org.sagebionetworks.migration.utils.MigrationTypeCountDiff;
import org.sagebionetworks.migration.utils.ToolMigrationUtils;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.tool.progress.BasicProgress;

/**
 * The migration client.
 *
 */
public class MigrationClient {
	
	static private Logger logger = LogManager.getLogger(MigrationClient.class);

	SynapseClientFactory factory;
	ExecutorService threadPool;

	/**
	 * New migration client.
	 * @param factory
	 */
	public MigrationClient(SynapseClientFactory factory) {
		if(factory == null) throw new IllegalArgumentException("Factory cannot be null");
		this.factory = factory;
		threadPool = Executors.newFixedThreadPool(1);
	}

	public List<MigrationType> getCommonMigrationTypes() throws SynapseException {
		SynapseAdminClient srcClient = factory.getSourceClient();
		SynapseAdminClient destClient = factory.getDestinationClient();

		MigrationTypeNames srcMigrationTypeNames = srcClient.getMigrationTypeNames();
		MigrationTypeNames destMigrationTypeNames = destClient.getMigrationTypeNames();

		return getCommonMigrationTypes(srcMigrationTypeNames, destMigrationTypeNames);
	}

	public List<MigrationType> getCommonPrimaryMigrationTypes() throws SynapseException {
		SynapseAdminClient srcClient = factory.getSourceClient();
		SynapseAdminClient destClient = factory.getDestinationClient();

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
			// TODO: remove when PLFM-4246 fixed, also fix MigrationClientTest.testGetCommonMigrationTypes()
			if (MigrationType.STORAGE_QUOTA.name().equals(typeName)) {
				continue;
			}
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
		setStatus(this.factory.getDestinationClient(), status, message);
	}

	/**
	 *
	 * @param status
	 * @param message
	 * @throws SynapseException
	 * @throws JSONObjectAdapterException
	 */
	public void setSourceStatus(StatusEnum status, String message) throws SynapseException, JSONObjectAdapterException {
		setStatus(this.factory.getSourceClient(), status, message);
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
	public boolean migrateWithRetry(int maxRetries, long maxBackupBatchSize, long minRangeSize, long timeoutMS) throws SynapseException, JSONObjectAdapterException {
		boolean failed = false;
		try {
			// First set the destination stack status to down
			setDestinationStatus(StatusEnum.READ_ONLY, "Staging is down for data migration");
			for (int i = 0; i < maxRetries; i++) {
				try {
					failed = migrate(minRangeSize, maxBackupBatchSize, timeoutMS);

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

	private boolean migrate(long minRangeSize, long maxBackupBatchSize, long timeoutMS) throws Exception {
		boolean failed;
		// Determine which types to migrate
		logger.info("Determining types to migrate...");
		List<MigrationType> typesToMigrate = this.getCommonMigrationTypes();
		List<MigrationType> primaryTypesToMigrate = this.getCommonPrimaryMigrationTypes();

		// Get the counts
		ConcurrentMigrationTypeCountsExecutor typeCountsExecutor = new ConcurrentMigrationTypeCountsExecutor(this.threadPool, this.factory, typesToMigrate, timeoutMS);

		ConcurrentExecutionResult<List<MigrationTypeCount>> migrationTypeCounts = typeCountsExecutor.getMigrationTypeCounts();
		List<MigrationTypeCount> startSourceCounts = migrationTypeCounts.getSourceResult();
		List<MigrationTypeCount> startDestinationCounts = migrationTypeCounts.getDestinationResult();

		// Build the metadata for each type
		List<TypeToMigrateMetadata> typesToMigrateMetadata = ToolMigrationUtils.buildTypeToMigrateMetadata(startSourceCounts, startDestinationCounts, primaryTypesToMigrate);

		// Display starting counts
		logger.info("Starting diffs in counts:");
		printDiffsInCounts(startSourceCounts, startDestinationCounts);

		// Actual migration
		this.migrateTypes(typesToMigrateMetadata, maxBackupBatchSize, minRangeSize, timeoutMS);

		// Print the final counts
		migrationTypeCounts = typeCountsExecutor.getMigrationTypeCounts();
		List<MigrationTypeCount> endSourceCounts = migrationTypeCounts.getSourceResult();
		List<MigrationTypeCount> endDestCounts = migrationTypeCounts.getDestinationResult();

		logger.info("Ending diffs in  counts:");
		printDiffsInCounts(endSourceCounts, endDestCounts);

		// If final sync (source is in read-only mode) then do a table checksum
		// Note: Destination is always in read-only during migration
		if (factory.getSourceClient().getCurrentStackStatus().getStatus() == StatusEnum.READ_ONLY) {
            doChecksumForMigratedTypes(factory.getSourceClient(), factory.getDestinationClient(), primaryTypesToMigrate);
        }

		// Exit on 1st success
		failed = false;
		return failed;
	}

	private void doChecksumForMigratedTypes(SynapseAdminClient source,
			SynapseAdminClient destination,
			List<MigrationType> typesToMigrate)
			throws SynapseException, JSONObjectAdapterException,
			RuntimeException, InterruptedException {
		logger.info("Final migration, checking table checksums");
		boolean isChecksumDiff = false;
		for (MigrationType t: typesToMigrate) {
			String srcTableChecksum = doAsyncChecksumForType(source, t);
			String destTableChecksum = doAsyncChecksumForType(destination, t);
			StringBuilder sb = new StringBuilder();
			sb.append("Migration type: ");
			sb.append(t);
			sb.append(": ");
			if (! srcTableChecksum.equals(destTableChecksum)) {
				isChecksumDiff = true;
				sb.append("\n*** Found table checksum difference. ***\n");
			} else {
				sb.append("Table checksums identical.");
			}
			logger.info(sb.toString());
		}
		if (isChecksumDiff) {
			throw new RuntimeException("Table checksum differences in final sync.");
		}
	}
	
	public String doAsyncChecksumForType(SynapseAdminClient client, MigrationType t) throws SynapseException, InterruptedException, JSONObjectAdapterException {
		String checksum = null;
		AsyncMigrationTypeChecksumRequest req = new AsyncMigrationTypeChecksumRequest();
		req.setType(t.name());
		AsyncMigrationWorker worker = new AsyncMigrationWorker(client, req, 900000);
		AdminResponse resp = worker.call();
		MigrationTypeChecksum res = (MigrationTypeChecksum)resp;
		checksum = res.getChecksum();
		return checksum;
	}

	public ConcurrentExecutionResult<String> doConcurrentChecksumForType(SynapseAdminClient source, SynapseAdminClient destination, MigrationType t)
		throws InterruptedException, AsyncMigrationException {
		AsyncMigrationTypeChecksumRequest srcReq = new AsyncMigrationTypeChecksumRequest();
		srcReq.setType(t.name());
		AsyncMigrationWorker srcWorker = new AsyncMigrationWorker(source, srcReq, 900000);
		AsyncMigrationTypeChecksumRequest destReq = new AsyncMigrationTypeChecksumRequest();
		destReq.setType(t.name());
		AsyncMigrationWorker destWorker = new AsyncMigrationWorker(source, srcReq, 900000);
		Future<AdminResponse> fSrcResp = threadPool.submit(srcWorker);
		Future<AdminResponse> fDestResp = threadPool.submit(destWorker);
		/* Order does not really matter, have to wait for slowest */
		waitForResults(fSrcResp);
		waitForResults(fDestResp);
		AdminResponse srcResp = getResponse(fSrcResp);
		AdminResponse destResp= getResponse(fDestResp);
		MigrationTypeChecksum srcChecksum = (MigrationTypeChecksum)srcResp;
		MigrationTypeChecksum destChecksum = (MigrationTypeChecksum)destResp;
		ConcurrentExecutionResult<String> result = new ConcurrentExecutionResult<String>();
		result.setSourceResult(srcChecksum.getChecksum());
		result.setDestinationResult(destChecksum.getChecksum());
		return result;
	}

	private void waitForResults(Future<AdminResponse> f) {
		while (! f.isDone()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// Should never happen!
				assert false;
			}
		}
	}

	private AdminResponse getResponse(Future<AdminResponse> f) throws InterruptedException, AsyncMigrationException {
		AdminResponse response = null;
		try {
			response = f.get();
		} catch (ExecutionException e) {
			logger.debug("Execution exception in table checksum.");
			throw (AsyncMigrationException)e.getCause();
		}
		return response;
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
	public void migrateTypes(List<TypeToMigrateMetadata> primaryTypes, long maxBackupBatchSize, long minRangeSize, long timeoutMS)
			throws Exception {

		// Each migration uses a different salt (same for each type)
		String salt = UUID.randomUUID().toString();
		
		List<DeltaData> deltaList = new LinkedList<DeltaData>();
		for (TypeToMigrateMetadata tm: primaryTypes) {
			DeltaData dd = calculateDeltaForType(tm, salt, minRangeSize);
			deltaList.add(dd);
		}
		
		// Delete any data in reverse order
		for(int i=deltaList.size()-1; i >= 0; i--){
			DeltaData dd = deltaList.get(i);
			long count =  dd.getCounts().getDelete();
			if(count > 0){
				deleteFromDestination(dd.getType(), dd.getDeleteTemp(), count, maxBackupBatchSize);
			}
		}

		// Now do all adds in the original order
		for(int i=0; i<deltaList.size(); i++){
			DeltaData dd = deltaList.get(i);
			long count = dd.getCounts().getCreate();
			if(count > 0){
				createUpdateInDestination(dd.getType(), dd.getCreateTemp(), count, maxBackupBatchSize, timeoutMS);
			}
		}

		// Now do all updates in the original order
		for(int i=0; i<deltaList.size(); i++){
			DeltaData dd = deltaList.get(i);
			long count = dd.getCounts().getUpdate();
			if(count > 0){
				createUpdateInDestination(dd.getType(), dd.getUpdateTemp(), count, maxBackupBatchSize, timeoutMS);
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
	private void createUpdateInDestination(MigrationType type, File createUpdateTemp, long count, long maxBackupBatchSize, long timeout) throws Exception {
		BufferedRowMetadataReader reader = new BufferedRowMetadataReader(new FileReader(createUpdateTemp));
		try{
			BasicProgress progress = new BasicProgress();
			CreateUpdateWorker worker = new CreateUpdateWorker(type, count, reader,progress,factory.getDestinationClient(), factory.getSourceClient(), maxBackupBatchSize, timeout);
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
	public DeltaData calculateDeltaForType(TypeToMigrateMetadata tm, String salt, long minRangeSize) throws Exception{

		// First, we find the delta ranges
		DeltaFinder finder = new DeltaFinder(tm, factory.getSourceClient(), factory.getDestinationClient(), salt, minRangeSize);
		DeltaRanges ranges = finder.findDeltaRanges();
		
		// the first thing we need to do is calculate the what needs to be created, updated, or deleted.
		// We need three temp file to keep track of the deltas
		File createTemp = File.createTempFile("create", ".tmp");
		File updateTemp = File.createTempFile("update", ".tmp");
		File deleteTemp = File.createTempFile("delete", ".tmp");
		
		// Calculate the deltas
		DeltaCounts counts = calculateDeltas(tm, ranges, minRangeSize, createTemp, updateTemp, deleteTemp);
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
	private DeltaCounts calculateDeltas(TypeToMigrateMetadata typeMeta, DeltaRanges ranges, long minRangeSize, File createTemp, File updateTemp, File deleteTemp)	throws Exception {

		BasicProgress sourceProgress = new BasicProgress();
		BasicProgress destProgress = new BasicProgress();
		BufferedRowMetadataWriter createOut = null;
		BufferedRowMetadataWriter updateOut = null;
		BufferedRowMetadataWriter deleteOut = null;
		try{
			createOut = new BufferedRowMetadataWriter(new FileWriter(createTemp));
			updateOut = new BufferedRowMetadataWriter(new FileWriter(updateTemp));
			deleteOut = new BufferedRowMetadataWriter(new FileWriter(deleteTemp));
			
			DeltaBuilder builder = new DeltaBuilder(factory, minRangeSize, typeMeta, ranges, createOut, updateOut, deleteOut);
			
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
	private void deleteFromDestination(MigrationType type, File deleteTemp, long count, long maxbackupBatchSize) throws Exception{
		BufferedRowMetadataReader reader = new BufferedRowMetadataReader(new FileReader(deleteTemp));
		try{
			BasicProgress progress = new BasicProgress();
			DeleteWorker worker = new DeleteWorker(type, count, reader, progress, factory.getDestinationClient(), maxbackupBatchSize);
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
	
	protected List<MigrationTypeCount> getTypeCounts(SynapseAdminClient conn, List<MigrationType> types) throws InterruptedException, JSONObjectAdapterException, SynapseException {
		List<MigrationTypeCount> typeCounts = new LinkedList<MigrationTypeCount>();
		for (MigrationType t: types) {
			try {
				MigrationTypeCount c = getTypeCount(conn, t);
				typeCounts.add(c);
			} catch (WorkerFailedException e) {
				// Unsupported types not added to list
			}
		}
		return typeCounts;
	}

	protected MigrationTypeCount getTypeCount(SynapseAdminClient conn, MigrationType type) throws SynapseException, InterruptedException, JSONObjectAdapterException {
		MigrationTypeCount res = null;
		try {
			res = conn.getTypeCount(type);
		} catch (SynapseException e) {
			AsyncMigrationTypeCountRequest req = new AsyncMigrationTypeCountRequest();
			req.setType(type.name());
			BasicProgress progress = new BasicProgress();
			AsyncMigrationWorker worker = new AsyncMigrationWorker(conn, req, 900000);
			AdminResponse resp = worker.call();
			res = (MigrationTypeCount)resp;
		}
		return res;
	}
}

