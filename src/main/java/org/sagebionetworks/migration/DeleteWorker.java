package org.sagebionetworks.migration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.sagebionetworks.migration.async.AsynchronousJobExecutor;
import org.sagebionetworks.repo.model.migration.DeleteListRequest;
import org.sagebionetworks.repo.model.migration.DeleteListResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.RowMetadata;
import org.sagebionetworks.tool.progress.BasicProgress;

/**
 * Deletes all ids on the passed list.
 * 
 * @author John
 *
 */
public class DeleteWorker implements Callable<Long>{
	
	MigrationType type;
	long count;
	Iterator<RowMetadata> iterator;
	BasicProgress progress;
	AsynchronousJobExecutor jobExecutor;
	long batchSize;
	
	

	public DeleteWorker(MigrationType type, long count,
			Iterator<RowMetadata> iterator, BasicProgress progress,
			AsynchronousJobExecutor jobExecutor, long batchSize) {
		super();
		this.type = type;
		this.iterator = iterator;
		this.progress.setCurrent(0L);
		this.progress.setTotal(count);
		this.progress = progress;
		this.jobExecutor = jobExecutor;
		this.batchSize = batchSize;
	}



	@Override
	public Long call() throws Exception {
		// Iterate and create batches.
		RowMetadata row = null;
		List<Long> batch = new LinkedList<Long>();
		long deletedCount = 0;
		long current = 0;
		while(iterator.hasNext()){
			row = iterator.next();
			current++;
			this.progress.setCurrent(current);
			if(row != null){
				batch.add(row.getId());
				if(batch.size() >= batchSize){
					Long c = this.deleteBatch(batch);
					deletedCount += c;
					batch.clear();
				}
			}
		}
		// If there is any data left in the batch send it
		if(batch.size() > 0){
			Long c = this.deleteBatch(batch);
			deletedCount += c;
			batch.clear();
		}
		progress.setDone();
		return deletedCount;
	}
	
	/**
	 * Execute a a delete job.
	 * @param batch
	 * @return
	 * @throws Exception
	 */
	public Long deleteBatch(List<Long> batch) throws Exception {
		DeleteListRequest deleteRequest = new DeleteListRequest();
		deleteRequest.setIdsToDelete(batch);
		deleteRequest.setMigrationType(type);
		DeleteListResponse response = jobExecutor.executeDestinationJob(deleteRequest, DeleteListResponse.class);
		return response.getDeleteCount();
	}
	
}
