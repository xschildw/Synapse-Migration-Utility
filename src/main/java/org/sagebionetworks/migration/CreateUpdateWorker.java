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

import com.google.common.collect.Iterators;

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
		Exception lastBatchException = null;
		long current = 0;
		Iterator<List<RowMetadata>> partitionIt = Iterators.partition(it, (int)batchSize);
		while(partitionIt.hasNext()) {
			List<RowMetadata> batchMetadata = partitionIt.next();
			List<Long> idBatch = new LinkedList<>();
			for(RowMetadata row: batchMetadata) {
				if(row.getId() != null) {
					idBatch.add(row.getId());
				}
			}
			try {
				migrateBatch(idBatch);
			} catch (Exception e) {
				/*
				 * If a batch fails we must continue processing the rest of the batches, since
				 * the fix for the failure is likely to be in one of the later batches. See:
				 * PLFM-3851
				 */
				lastBatchException = e;
			}
			current += idBatch.size();
			progress.setCurrent(current);
		}

		if (lastBatchException != null) {
			// Throwing the exception indicates that this type must be re-migrated to resolve the failures.
			throw lastBatchException;
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
		if(ids.isEmpty()) {
			return 0L;
		}
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