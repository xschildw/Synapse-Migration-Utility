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

import com.google.common.collect.Iterators;

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
		this.progress = progress;
		this.progress.setCurrent(0L);
		this.progress.setTotal(count);
		this.jobExecutor = jobExecutor;
		this.batchSize = batchSize;
	}



	@Override
	public Long call() throws Exception {
		// Iterate and create batches.
		long count = 0;
		Iterator<List<RowMetadata>> partitionIt = Iterators.partition(iterator, (int) batchSize);
		while(partitionIt.hasNext()) {
			List<RowMetadata> metaBatch = partitionIt.next();
			List<Long> idBatch = new LinkedList<>();
			for(RowMetadata row: metaBatch) {
				if(row.getId() != null) {
					idBatch.add(row.getId());
				}
			}
			count += deleteBatch(idBatch);
		}
		progress.setDone();
		return count;
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
