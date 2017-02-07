package org.sagebionetworks.migration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;
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
	
	static private Logger log = LogManager.getLogger(MigrationClient.class);

	SynapseClientFactory factory;
	ExecutorService threadPool;
	List<Exception> deferredExceptions;

	/**
	 * New migration client.
	 * @param factory
	 */
	public MigrationClient(SynapseClientFactory factory) {
		if(factory == null) throw new IllegalArgumentException("Factory cannot be null");
		this.factory = factory;
		threadPool = Executors.newFixedThreadPool(1);
		deferredExceptions = new ArrayList<Exception>();
	}

	public List<MigrationType> getCommonMigrationTypes() throws SynapseException {
		SynapseAdminClient srcClient = factory.getSourceClient();
		SynapseAdminClient destClient = factory.getDestinationClient();

		MigrationTypeNames srcMigrationTypeNames = srcClient.getMigrationTypeNames();
		MigrationTypeNames destMigrationTypeNames = destClient.getMigrationTypeNames();

		List<String> commonTypeNames = getCommonTypeNames(srcMigrationTypeNames, destMigrationTypeNames);

		List<MigrationType> commonTypes = new LinkedList<MigrationType>();
		for (String t: commonTypeNames) {
			commonTypes.add(MigrationType.valueOf(t));
		}
		return commonTypes;
	}

	public List<MigrationType> getCommonPrimaryMigrationTypes() throws SynapseException {
		SynapseAdminClient srcClient = factory.getSourceClient();
		SynapseAdminClient destClient = factory.getDestinationClient();

		MigrationTypeNames srcMigrationTypeNames = srcClient.getPrimaryTypeNames();
		MigrationTypeNames destMigrationTypeNames = destClient.getPrimaryTypeNames();

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
	public boolean migrate(int maxRetries, long batchSize, long timeoutMS) throws SynapseException, JSONObjectAdapterException {
		boolean failed = false;
		try {
			// First set the destination stack status to down
			setDestinationStatus(StatusEnum.READ_ONLY, "Staging is down for data migration");
			for (int i = 0; i < maxRetries; i++) {
				try {
					// Determine which types to migrate
					List<MigrationType> typesToMigrate = this.getCommonMigrationTypes();
					List<MigrationType> primaryTypesToMigrate = this.getCommonPrimaryMigrationTypes();
					// Get the counts
					// TODO: Replace by async when supported by backend
					List<MigrationTypeCount> startSourceCounts = getTypeCounts(factory.getSourceClient(), typesToMigrate);
					List<MigrationTypeCount> startDestCounts = getTypeCounts(factory.getDestinationClient(), typesToMigrate);
					// Build the metadata for each type
					List<TypeToMigrateMetadata> typesToMigrateMetadata = ToolMigrationUtils.buildTypeToMigrateMetadata(startSourceCounts, startDestCounts, primaryTypesToMigrate);

					// Display starting counts
					log.info("Starting diffs in counts:");
					printDiffsInCounts(startSourceCounts, startDestCounts);

					// Actual migration
					this.migrateTypes(typesToMigrateMetadata, batchSize, timeoutMS);

					// Print the final counts
					List<MigrationTypeCount> endSourceCounts = getTypeCounts(factory.getSourceClient(), typesToMigrate);
					List<MigrationTypeCount> endDestCounts = getTypeCounts(factory.getDestinationClient(), typesToMigrate);
					log.info("Ending diffs in  counts:");
					printDiffsInCounts(endSourceCounts, endDestCounts);

					// If final sync (source is in read-only mode) then do a table checksum
					// Note: Destination is always in read-only during migration
					if (factory.getSourceClient().getCurrentStackStatus().getStatus() == StatusEnum.READ_ONLY) {
						doChecksumForMigratedTypes(factory.getSourceClient(), factory.getDestinationClient(), typesToMigrateMetadata);
					}

					// Exit on 1st success
					failed = false;
				} catch (Exception e) {
					failed = true;
					log.error("Failed at attempt: " + i + " with error " + e.getMessage(), e);
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

	private void doChecksumForMigratedTypes(SynapseAdminClient source,
			SynapseAdminClient destination,
			List<TypeToMigrateMetadata> typesToMigrateMetadata)
			throws SynapseException, JSONObjectAdapterException,
			RuntimeException, InterruptedException {
		log.info("Final migration, checking table checksums");
		boolean isChecksumDiff = false;
		for (TypeToMigrateMetadata t: typesToMigrateMetadata) {
			String srcTableChecksum = doAsyncChecksumForType(source, t.getType());
			String destTableChecksum = doAsyncChecksumForType(destination, t.getType());
			StringBuilder sb = new StringBuilder();
			sb.append("Migration type: ");
			sb.append(t.getType());
			sb.append(": ");
			if (! srcTableChecksum.equals(destTableChecksum)) {
				isChecksumDiff = true;
				sb.append("\n*** Found table checksum difference. ***\n");
			} else {
				sb.append("Table checksums identical.");
			}
			log.info(sb.toString());
		}
		if (isChecksumDiff) {
			throw new RuntimeException("Table checksum differences in final sync.");
		}
	}
	
	private String doChecksumForTypeWithOneRetry(SynapseAdminClient client, MigrationType t) throws SynapseException, JSONObjectAdapterException {
		String checksum = null;
		try {
			checksum = client.getChecksumForType(t).getChecksum();
		} catch (SynapseException e) {
			if (e.getCause() instanceof SocketTimeoutException) {
				checksum = client.getChecksumForType(t).getChecksum();
			} else {
				throw e;
			}
		}
		return checksum;
	}
	
	public String doAsyncChecksumForType(SynapseAdminClient client, MigrationType t) throws SynapseException, InterruptedException, JSONObjectAdapterException {
		String checksum = null;
		AsyncMigrationTypeChecksumRequest req = new AsyncMigrationTypeChecksumRequest();
		req.setType(t.name());
		BasicProgress progress = new BasicProgress();
		AsyncMigrationWorker worker = new AsyncMigrationWorker(client, req, 900000, progress);
		AdminResponse resp = worker.call();
		MigrationTypeChecksum res = (MigrationTypeChecksum)resp;
		checksum = res.getChecksum();
		return checksum;
	}

	/**
	 * Does the actual migration work.
	 *
	 * @param batchSize
	 * @param timeoutMS
	 * @param primaryTypes
	 * @throws Exception
	 */
	public void migrateTypes(List<TypeToMigrateMetadata> primaryTypes, long batchSize, long timeoutMS)
			throws Exception {

		// Each migration uses a different salt (same for each type)
		String salt = UUID.randomUUID().toString();
		
		List<DeltaData> deltaList = new LinkedList<DeltaData>();
		for (TypeToMigrateMetadata tm: primaryTypes) {
			DeltaData dd = calculateDeltaForType(tm, salt, batchSize);
			deltaList.add(dd);
		}
		
		// Delete any data in reverse order
		for(int i=deltaList.size()-1; i >= 0; i--){
			DeltaData dd = deltaList.get(i);
			long count =  dd.getCounts().getDelete();
			if(count > 0){
				deleteFromDestination(dd.getType(), dd.getDeleteTemp(), count, batchSize);
			}
		}

		// Now do all adds in the original order
		for(int i=0; i<deltaList.size(); i++){
			DeltaData dd = deltaList.get(i);
			long count = dd.getCounts().getCreate();
			if(count > 0){
				createUpdateInDestination(dd.getType(), dd.getCreateTemp(), count, batchSize, timeoutMS);
			}
		}

		// Now do all updates in the original order
		for(int i=0; i<deltaList.size(); i++){
			DeltaData dd = deltaList.get(i);
			long count = dd.getCounts().getUpdate();
			if(count > 0){
				createUpdateInDestination(dd.getType(), dd.getUpdateTemp(), count, batchSize, timeoutMS);
			}
		}
	}

	/**
	 * Create or update
	 * 
	 * @param type
	 * @param createUpdateTemp
	 * @param count
	 * @param batchSize
	 * @param timeout
	 * @throws Exception
	 */
	private void createUpdateInDestination(MigrationType type, File createUpdateTemp, long count, long batchSize, long timeout) throws Exception {
		BufferedRowMetadataReader reader = new BufferedRowMetadataReader(new FileReader(createUpdateTemp));
		try{
			BasicProgress progress = new BasicProgress();
			CreateUpdateWorker worker = new CreateUpdateWorker(type, count, reader,progress,factory.getDestinationClient(), factory.getSourceClient(), batchSize, timeout);
			Future<Long> future = this.threadPool.submit(worker);
			while(!future.isDone()){
				// Log the progress
				String message = progress.getMessage();
				if(message == null){
					message = "";
				}
				log.info("Creating/updating data for type: "+type.name()+" Progress: "+progress.getCurrentStatus()+" "+message);
				Thread.sleep(2000);
			}
				Long counts = future.get();
				log.info("Creating/updating the following counts for type: "+type.name()+" Counts: "+counts);
		} finally {
			reader.close();
		}
	}

	private void printDiffsInCounts(List<MigrationTypeCount> srcCounts, List<MigrationTypeCount> destCounts) {
		List<MigrationTypeCountDiff> diffs = ToolMigrationUtils.getMigrationTypeCountDiffs(srcCounts, destCounts);
		for (MigrationTypeCountDiff d: diffs) {
			// Missing at source
			if (d.getDelta() == null) {
				log.info("\t" + d.getType().name() + "\tNA\t" + d.getDestinationCount());
			} else if (d.getDelta() != 0L) {
					log.info("\t" + d.getType().name() + ":\t" + d.getDelta() + "\t" + d.getSourceCount() + "\t" + d.getDestinationCount());
			}
		}
	}

	/**
	 * Calculate deltas for one type
	 *
	 * @param tm
	 * @param salt
	 * @param batchSize
	 * @return
	 * @throws Exception
	 */
	public DeltaData calculateDeltaForType(TypeToMigrateMetadata tm, String salt, long batchSize) throws Exception{

		// First, we find the delta ranges
		DeltaFinder finder = new DeltaFinder(tm, factory.getSourceClient(), factory.getDestinationClient(), salt, batchSize);
		DeltaRanges ranges = finder.findDeltaRanges();
		
		// the first thing we need to do is calculate the what needs to be created, updated, or deleted.
		// We need three temp file to keep track of the deltas
		File createTemp = File.createTempFile("create", ".tmp");
		File updateTemp = File.createTempFile("update", ".tmp");
		File deleteTemp = File.createTempFile("delete", ".tmp");
		
		// Calculate the deltas
		DeltaCounts counts = calculateDeltas(tm, ranges, batchSize, createTemp, updateTemp, deleteTemp);
		return new DeltaData(tm.getType(), createTemp, updateTemp, deleteTemp, counts);
		
	}

	/**
	 * Calculate the deltas
	 * @param typeMeta
	 * @param batchSize
	 * @param createTemp
	 * @param updateTemp
	 * @param deleteTemp
	 * @throws SynapseException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private DeltaCounts calculateDeltas(TypeToMigrateMetadata typeMeta, DeltaRanges ranges, long batchSize, File createTemp, File updateTemp, File deleteTemp)	throws Exception {

		BasicProgress sourceProgress = new BasicProgress();
		BasicProgress destProgress = new BasicProgress();
		BufferedRowMetadataWriter createOut = null;
		BufferedRowMetadataWriter updateOut = null;
		BufferedRowMetadataWriter deleteOut = null;
		try{
			createOut = new BufferedRowMetadataWriter(new FileWriter(createTemp));
			updateOut = new BufferedRowMetadataWriter(new FileWriter(updateTemp));
			deleteOut = new BufferedRowMetadataWriter(new FileWriter(deleteTemp));
			
			DeltaBuilder builder = new DeltaBuilder(factory, batchSize, typeMeta, ranges, createOut, updateOut, deleteOut);
			
			// Unconditional inserts
			long insCount = builder.addInsertsFromSource();
			
			// Unconditional deletes
			long delCount = builder.addDeletesAtDestination();;
			
			// Deep comparison of update box
			DeltaCounts counts = builder.addDifferencesBetweenSourceAndDestination();
			counts.setCreate(counts.getCreate()+insCount);
			counts.setDelete(counts.getDelete()+delCount);
			
			log.info("Calculated the following counts for type: "+typeMeta.getType().name()+" Counts: "+counts);
			
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
	private void deleteFromDestination(MigrationType type, File deleteTemp, long count, long batchSize) throws Exception{
		BufferedRowMetadataReader reader = new BufferedRowMetadataReader(new FileReader(deleteTemp));
		try{
			BasicProgress progress = new BasicProgress();
			DeleteWorker worker = new DeleteWorker(type, count, reader, progress, factory.getDestinationClient(), batchSize);
			Future<Long> future = this.threadPool.submit(worker);
			while(!future.isDone()){
				// Log the progress
				log.info("Deleting data for type: "+type.name()+" Progress: "+progress.getCurrentStatus());
				Thread.sleep(2000);
			}
			Long counts = future.get();
			log.info("Deleted the following counts for type: "+type.name()+" Counts: "+counts);
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
			AsyncMigrationWorker worker = new AsyncMigrationWorker(conn, req, 900000, progress);
			AdminResponse resp = worker.call();
			res = (MigrationTypeCount)resp;
		}
		return res;
	}
}

