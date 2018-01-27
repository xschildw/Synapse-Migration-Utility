package org.sagebionetworks.migration;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.async.AsynchronousJobExecutor;
import org.sagebionetworks.repo.model.migration.AdminRequest;
import org.sagebionetworks.repo.model.migration.DeleteListRequest;
import org.sagebionetworks.repo.model.migration.DeleteListResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.RowMetadata;
import org.sagebionetworks.tool.progress.BasicProgress;

import com.google.common.collect.Lists;

/**
 * Test for DeleteWorker
 * 
 * @author John
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DeleteWorkerTest {
	
	@Mock
	BasicProgress mockProgress;
	@Mock
	AsynchronousJobExecutor mockJobExecutor;
	
	MigrationType type;
	long count;
	Iterator<RowMetadata> iterator;
	long batchSize;
	
	@Before
	public void before() throws AsyncMigrationException, InterruptedException {
		type = MigrationType.NODE;
		count = 3;
		batchSize = 4;
		DeleteListResponse response = new DeleteListResponse();
		response.setDeleteCount(count);
		when(mockJobExecutor.executeDestinationJob(any(DeleteListRequest.class), any())).thenReturn(response);
		doThrow(new IllegalStateException("Delete should never be called against source")).when(mockJobExecutor)
				.executeSourceJob(any(AdminRequest.class), any());
	}
	
	/***
	 * Create a new worker using the members of this class.
	 * @return
	 */
	DeleteWorker createNewWorker() {
		List<RowMetadata> metadata = new LinkedList<>();
		for(long i=0; i<count; i++) {
			RowMetadata row = new RowMetadata();
			row.setId(i);
			metadata.add(row);
		}
		iterator = metadata.iterator();
		return new DeleteWorker(type, count, iterator, mockProgress, mockJobExecutor, batchSize);
	}
	
	@Test
	public void testDeleteBatch() throws Exception {
		List<Long> toDelet = Lists.newArrayList(111L,222L);
		DeleteWorker worker = createNewWorker();
		long deleteCount = worker.deleteBatch(toDelet);
		assertEquals(count, deleteCount);
		DeleteListRequest expectedRequest = new DeleteListRequest();
		expectedRequest.setIdsToDelete(toDelet);
		expectedRequest.setMigrationType(type);
		verify(mockJobExecutor).executeDestinationJob(expectedRequest, DeleteListResponse.class);
	}
	
	
	@Test
	public void testCallCountLessBatchSize() throws Exception {
		count = 3;
		batchSize = count+1;
		DeleteWorker worker = createNewWorker();
		// call under test
		long returnCount = worker.call();
		assertEquals(count, returnCount);
		// with a batch size larger then the count all data should be in one batch.
		DeleteListRequest expectedRequest = new DeleteListRequest();
		expectedRequest.setIdsToDelete(Lists.newArrayList(0L,1L,2L));
		expectedRequest.setMigrationType(type);
		verify(mockJobExecutor).executeDestinationJob(expectedRequest, DeleteListResponse.class);
	}
	
	@Test
	public void testCallCountGreaterBatchSize() throws Exception {
		count = 3;
		batchSize = count-1;
		DeleteWorker worker = createNewWorker();
		// call under test
		long returnCount = worker.call();
		assertEquals(count*2, returnCount);
		// with a batch size larger then the count all data should be in one batch.
		DeleteListRequest expectedRequest = new DeleteListRequest();
		expectedRequest.setIdsToDelete(Lists.newArrayList(0L,1L));
		expectedRequest.setMigrationType(type);
		verify(mockJobExecutor).executeDestinationJob(expectedRequest, DeleteListResponse.class);
		// second batch should contain one row.
		expectedRequest = new DeleteListRequest();
		expectedRequest.setIdsToDelete(Lists.newArrayList(2L));
		expectedRequest.setMigrationType(type);
		verify(mockJobExecutor).executeDestinationJob(expectedRequest, DeleteListResponse.class);
	}

}
