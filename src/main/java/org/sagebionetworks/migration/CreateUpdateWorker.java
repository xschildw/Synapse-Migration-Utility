package org.sagebionetworks.migration;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.migration.async.AsynchronousJobExecutor;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;
import org.sagebionetworks.repo.model.migration.BackupTypeListRequest;
import org.sagebionetworks.repo.model.migration.BackupTypeResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.RestoreTypeRequest;
import org.sagebionetworks.repo.model.migration.RestoreTypeResponse;
import org.sagebionetworks.repo.model.migration.RowMetadata;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.sagebionetworks.tool.progress.BasicProgress;

public class CreateUpdateWorker implements Callable<Long> {
		
	MigrationType type;
	BackupAliasType aliasType;
	long count;
	Iterator<RowMetadata> iterator;
	BasicProgress progress;
	AsynchronousJobExecutor jobExecutor;
	long batchSize;

	/**
	 * 
	 * @param type - The type to be migrated.
	 * @param iterator - Abstraction for iterating over the objects to be migrated.
	 * @param progress - The worker will update the progress objects so its progress can be monitored externally.
	 * @param destClient - A handle to the destination SynapseAdministration client. Data will be pushed to the destination.
	 * @param sourceClient - A handle to the source SynapseAdministration client. Data will be pulled from the source.
	 * @param batchSize - Data is migrated in batches.  This controls the size of the batches.
	 * @param timeoutMS - How long should the worker wait for Daemon job to finish its task before timing out in milliseconds.
	 * using this number as the denominator. An attempt will then be made to retry the migration of each sub-batch in an attempt to isolate the problem.
	 * If this is set to less than 2, then no re-try will be attempted.
	 */
	public CreateUpdateWorker(MigrationType type, BackupAliasType aliasType, Iterator<RowMetadata> iterator, BasicProgress progress,
			AsynchronousJobExecutor jobExecutor, long batchSize) {
		super();
		this.type = type;
		this.aliasType = aliasType;
		this.iterator = iterator;
		this.progress = progress;
		this.progress.setCurrent(0);
		this.progress.setTotal(count);
		this.jobExecutor = jobExecutor;
		this.batchSize = batchSize;
	}

	@Override
	public Long call() throws Exception {
		Long updateCount = backupAsBatch(this.iterator);
		progress.setDone();
		return updateCount;
	}
	
	/**
	 * Backup a single bucket
	 * @param it
	 * @return
	 * @throws Exception
	 */
	private long backupAsBatch(Iterator<RowMetadata> it) throws Exception{
		// Iterate and create batches.
		Long id = null;
		List<Long> batch = new LinkedList<Long>();
		Exception migrateBatchException = null;
		long updateCount = 0;
		while(it.hasNext()){
			id = it.next().getId();
			if(id != null){
				batch.add(id);
				if(batch.size() >= batchSize){
					try {
						migrateBatch(batch);
					} catch (Exception e) {
						migrateBatchException = e;
					}
					updateCount += batch.size();
					batch.clear();
				}
			}
		}
		// If there is any data left in the batch send it
		if(batch.size() > 0){
			try {
				migrateBatch(batch);
			}  catch (Exception e) {
				migrateBatchException = e;
			}
			updateCount += batch.size();
			batch.clear();
		}
		if (migrateBatchException != null) {
			throw migrateBatchException;
		}
		return updateCount;
	}
	

	/**
	 * Attempt to migrateWithRetry a single batch.
	 * @param ids
	 * @throws JSONObjectAdapterException
	 * @throws SynapseException
	 * @throws InterruptedException
	 */
	public Long migrateBatch(List<Long> ids) throws JSONObjectAdapterException, SynapseException, InterruptedException {
		int listSize = ids.size();
		progress.setMessage("Starting backup job for "+listSize+" objects");
		
		// execute the backup job on the source.
		BackupTypeListRequest backupRequest = new BackupTypeListRequest();
		backupRequest.setAliasType(this.aliasType);
		backupRequest.setBatchSize(this.batchSize);
		backupRequest.setMigrationType(this.type);
		backupRequest.setRowIdsToBackup(ids);
		BackupTypeResponse backupResponse = jobExecutor.executeSourceJob(backupRequest, BackupTypeResponse.class);
		
		progress.setMessage("Starting restore job for "+backupResponse.getBackupFileKey());
		
		// Execute the restore on the destination.
		RestoreTypeRequest restoreRequest = new RestoreTypeRequest();
		restoreRequest.setAliasType(aliasType);
		restoreRequest.setBatchSize(this.batchSize);
		restoreRequest.setMigrationType(this.type);
		restoreRequest.setBackupFileKey(backupResponse.getBackupFileKey());
		RestoreTypeResponse restoreResponse = jobExecutor.executeDestinationJob(restoreRequest, RestoreTypeResponse.class);

		// Update the progress
		progress.setMessage("Finished restore for "+restoreResponse.getRestoredRowCount()+" rows");
		progress.setCurrent(progress.getCurrent()+listSize);
		return (long) (listSize);
	}

}