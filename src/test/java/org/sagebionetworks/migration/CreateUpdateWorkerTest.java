package org.sagebionetworks.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
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
import org.sagebionetworks.tool.progress.BasicProgress;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class CreateUpdateWorkerTest {

	@Mock
	AsynchronousJobExecutor mockJobExecutor;
	@Mock
	BasicProgress mockProgress;
	
	MigrationType type;
	long count;
	BackupAliasType aliasType;
	List<RowMetadata> metadata;
	Iterator<RowMetadata> iterator;
	long batchSize;
	String backupFile;
	
	@Before
	public void before() throws Exception {
		type = MigrationType.NODE;
		count = 3;
		aliasType = BackupAliasType.TABLE_NAME;
		backupFile = "someFile";
		// mock bakcup
		BackupTypeResponse reponse = new BackupTypeResponse();
		reponse.setBackupFileKey(backupFile);
		when(mockJobExecutor.executeSourceJob(any(BackupTypeListRequest.class), any())).thenReturn(reponse);
		// mock restore
		RestoreTypeResponse restoreResponse = new RestoreTypeResponse();
		restoreResponse.setRestoredRowCount(count);
		when(mockJobExecutor.executeDestinationJob(any(RestoreTypeRequest.class), any())).thenReturn(restoreResponse);
		// mock delete
		DeleteListResponse deleteReponse = new DeleteListResponse();
		deleteReponse.setDeleteCount(count);
		when(mockJobExecutor.executeDestinationJob(any(DeleteListRequest.class), any())).thenReturn(restoreResponse);
	}
	
	/***
	 * Create a new worker using the members of this class.
	 * @return
	 */
	CreateUpdateWorker createNewWorker() {
		List<RowMetadata> metadata = new LinkedList<>();
		for(long i=0; i<count; i++) {
			RowMetadata row = new RowMetadata();
			row.setId(i);
			metadata.add(row);
		}
		iterator = metadata.iterator();
		return new CreateUpdateWorker(type, count, aliasType, iterator, mockProgress, mockJobExecutor, batchSize);
	}
	
	@Test
	public void testMigrateBatch() throws Exception {
		CreateUpdateWorker worker = createNewWorker();
		List<Long> ids = Lists.newArrayList(111L,222L);
		// call under test
		long resultCount = worker.migrateBatch(ids);
		assertEquals(2L, resultCount);
		BackupTypeListRequest expectedBackupRequest = new BackupTypeListRequest();
		expectedBackupRequest.setAliasType(aliasType);
		expectedBackupRequest.setBatchSize(batchSize);
		expectedBackupRequest.setMigrationType(type);
		expectedBackupRequest.setRowIdsToBackup(ids);
		// backup on the source
		verify(mockJobExecutor).executeSourceJob(expectedBackupRequest, BackupTypeResponse.class);
		
		// restore on the destination.
		RestoreTypeRequest expectedRestore = new RestoreTypeRequest();
		expectedRestore.setAliasType(aliasType);
		expectedRestore.setBatchSize(batchSize);
		expectedRestore.setMigrationType(type);
		expectedRestore.setBackupFileKey(backupFile);
		verify(mockJobExecutor).executeDestinationJob(expectedRestore, RestoreTypeResponse.class);
		
		verify(mockProgress, times(3)).setMessage(anyString());

	}
	
	@Test
	public void testMigrateBatchEmpty() throws Exception {
		CreateUpdateWorker worker = createNewWorker();
		List<Long> ids = new LinkedList<>();
		// call under test
		long resultCount = worker.migrateBatch(ids);
		assertEquals(0L, resultCount);
		verify(mockJobExecutor, never()).executeSourceJob(any(BackupTypeListRequest.class), any());
		verify(mockJobExecutor, never()).executeDestinationJob(any(BackupTypeListRequest.class), any());
		verify(mockProgress, never()).setMessage(anyString());
	}
	
	@Test
	public void testMigrateBatchRestoreFailed() throws Exception {
		CreateUpdateWorker worker = createNewWorker();
		List<Long> ids = Lists.newArrayList(111L,222L);
		// setup a restore failures
		AsyncMigrationException failure = new AsyncMigrationException("something failed");
		doThrow(failure).when(mockJobExecutor).executeDestinationJob(any(RestoreTypeRequest.class), any());
		// call under test
		try {
			worker.migrateBatch(ids);
			fail();
		} catch (Exception e) {
			// the exception should be re-thrown.
			assertEquals(failure, e);
		}
		// Exception should trigger the batch to be deleted on the destination.
		DeleteListRequest expectedDelete = new DeleteListRequest();
		expectedDelete.setIdsToDelete(ids);
		expectedDelete.setMigrationType(type);
		verify(mockJobExecutor).executeDestinationJob(expectedDelete, DeleteListResponse.class);
		verify(mockProgress, times(3)).setMessage(anyString());
	}
	
	@Test
	public void testCallBatchLargerThanCount() throws Exception {
		this.count = 3;
		this.batchSize = this.count+1;
		CreateUpdateWorker worker = createNewWorker();
		// call under test
		long resultCount = worker.call();
		assertEquals(count, resultCount);
		BackupTypeListRequest expectedBackupRequest = new BackupTypeListRequest();
		expectedBackupRequest.setAliasType(aliasType);
		expectedBackupRequest.setBatchSize(batchSize);
		expectedBackupRequest.setMigrationType(type);
		expectedBackupRequest.setRowIdsToBackup(Lists.newArrayList(0L,1L,2L));
		// backup on the source
		verify(mockJobExecutor).executeSourceJob(expectedBackupRequest, BackupTypeResponse.class);
		verify(mockJobExecutor).executeDestinationJob(any(BackupTypeListRequest.class), any());

	}
	
	@Test
	public void testCallBatchSmallerThanCount() throws Exception {
		this.count = 3;
		this.batchSize = this.count-1;
		CreateUpdateWorker worker = createNewWorker();
		// call under test
		long resultCount = worker.call();
		assertEquals(count, resultCount);
		BackupTypeListRequest one = new BackupTypeListRequest();
		one.setAliasType(aliasType);
		one.setBatchSize(batchSize);
		one.setMigrationType(type);
		one.setRowIdsToBackup(Lists.newArrayList(0L,1L));
		
		BackupTypeListRequest two = new BackupTypeListRequest();
		two.setAliasType(aliasType);
		two.setBatchSize(batchSize);
		two.setMigrationType(type);
		two.setRowIdsToBackup(Lists.newArrayList(2L));
		
		// backup on the source should be called twice
		verify(mockJobExecutor).executeSourceJob(one, BackupTypeResponse.class);
		verify(mockJobExecutor).executeSourceJob(two, BackupTypeResponse.class);
		verify(mockJobExecutor, times(2)).executeDestinationJob(any(BackupTypeListRequest.class), any());
	}
	
	/**
	 * Part of the fix for PLFM-3851 is to ensure that if on batch fails, the rest of 
	 * the batches must still be executed before throwing the exception.
	 * @throws Exception
	 */
	@Test
	public void testCallFailure() throws Exception {
		this.count = 3;
		this.batchSize = this.count-1;
		CreateUpdateWorker worker = createNewWorker();
		
		AsyncMigrationException failure = new AsyncMigrationException("something failed");
		// setup failure on restore.
		doThrow(failure).when(mockJobExecutor).executeDestinationJob(any(RestoreTypeRequest.class), any());
		
		// call under test
		try {
			worker.call();
			fail();
		} catch (Exception e) {
			assertEquals(failure, e);
		}
		BackupTypeListRequest one = new BackupTypeListRequest();
		one.setAliasType(aliasType);
		one.setBatchSize(batchSize);
		one.setMigrationType(type);
		one.setRowIdsToBackup(Lists.newArrayList(0L,1L));
		
		BackupTypeListRequest two = new BackupTypeListRequest();
		two.setAliasType(aliasType);
		two.setBatchSize(batchSize);
		two.setMigrationType(type);
		two.setRowIdsToBackup(Lists.newArrayList(2L));
		
		verify(mockJobExecutor).executeSourceJob(one, BackupTypeResponse.class);
		// even though there was a failure, the second batch must still be executed.
		verify(mockJobExecutor).executeSourceJob(two, BackupTypeResponse.class);
		
		RestoreTypeRequest expectedRestore = new RestoreTypeRequest();
		expectedRestore.setAliasType(aliasType);
		expectedRestore.setBatchSize(batchSize);
		expectedRestore.setMigrationType(type);
		expectedRestore.setBackupFileKey(backupFile);
		verify(mockJobExecutor, times(2)).executeDestinationJob(expectedRestore, RestoreTypeResponse.class);
		
		// both batches should be deleted
		DeleteListRequest deleteOne = new DeleteListRequest();
		deleteOne.setIdsToDelete(Lists.newArrayList(0L,1L));
		deleteOne.setMigrationType(type);
		verify(mockJobExecutor).executeDestinationJob(deleteOne, DeleteListResponse.class);
		
		DeleteListRequest deleteTwo = new DeleteListRequest();
		deleteTwo.setIdsToDelete(Lists.newArrayList(2L));
		deleteTwo.setMigrationType(type);
		verify(mockJobExecutor).executeDestinationJob(deleteTwo, DeleteListResponse.class);
	}
}
