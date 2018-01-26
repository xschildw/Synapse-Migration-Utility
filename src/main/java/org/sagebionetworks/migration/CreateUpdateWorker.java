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
import org.sagebionetworks.repo.model.migration.DeleteListRequest;
import org.sagebionetworks.repo.model.migration.DeleteListResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.RestoreTypeRequest;
import org.sagebionetworks.repo.model.migration.RestoreTypeResponse;
import org.sagebionetworks.repo.model.migration.RowMetadata;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.sagebionetworks.tool.progress.BasicProgress;

public class CreateUpdateWorker implements Callable<Long> {
		
	MigrationType type;
	BackupAliasType aliasType;
	Iterator<RowMetadata> iterator;
	BasicProgress progress;
	AsynchronousJobExecutor jobExecutor;
	long batchSize;

	/**
	 */
	public CreateUpdateWorker(MigrationType type, long count, BackupAliasType aliasType, Iterator<RowMetadata> iterator, BasicProgress progress,
			AsynchronousJobExecutor jobExecutor, long batchSize) {
		super();
		this.type = type;
		this.aliasType = aliasType;
		this.iterator = iterator;
		this.progress = progress;
		this.progress.setCurrent(0L);
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
	protected long backupAsBatch(Iterator<RowMetadata> it) throws Exception{
		// Iterate and create batches.
		Long id = null;
		List<Long> batch = new LinkedList<Long>();
		Exception migrateBatchException = null;
		long current = 0;
		while(it.hasNext()){
			current++;
			progress.setCurrent(current);
			id = it.next().getId();
			if(id != null){
				batch.add(id);
				if(batch.size() >= batchSize){
					try {
						migrateBatch(batch);
					} catch (Exception e) {
						migrateBatchException = e;
					}
					batch = new LinkedList<Long>();
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
		}
		if (migrateBatchException != null) {
			throw migrateBatchException;
		}
		return current;
	}
	

	/**
	 * Attempt to migrate a single batch.
	 * @param ids
	 * @throws JSONObjectAdapterException
	 * @throws SynapseException
	 * @throws InterruptedException
	 */
	protected Long migrateBatch(List<Long> ids) throws JSONObjectAdapterException, SynapseException, InterruptedException {
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
		RestoreTypeResponse restoreResponse;
		try {
			restoreResponse = jobExecutor.executeDestinationJob(restoreRequest, RestoreTypeResponse.class);
			// Update the progress
			progress.setMessage("Finished restore for "+restoreResponse.getRestoredRowCount()+" rows");
			progress.setCurrent(progress.getCurrent()+listSize);
			return (long) (listSize);
		} catch (AsyncMigrationException e) {
			// See PLFM-3851. When the restore fails, delete all data associated with this batch and then throw the exception.
			progress.setMessage("Failed: "+e.getMessage()+" deleting all data for this batch");
			DeleteListRequest deleteRequest = new DeleteListRequest();
			deleteRequest.setIdsToDelete(ids);
			deleteRequest.setMigrationType(type);
			jobExecutor.executeDestinationJob(deleteRequest, DeleteListResponse.class);
			throw e;
		}
	}

}