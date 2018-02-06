package org.sagebionetworks.migration.async;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;
import org.sagebionetworks.repo.model.migration.AdminRequest;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.RestoreTypeRequest;
import org.sagebionetworks.repo.model.migration.RestoreTypeResponse;

@RunWith(MockitoJUnitRunner.class)
public class DestinationJobExecutorImplTest {

	@Mock
	Configuration mockConfig;
	@Mock
	AsynchronousJobExecutor mockAsynchronousJobExecutor;
	@Mock
	Future mockFuture;
	
	DestinationJobExecutorImpl destinationExecutor;
	
	MigrationType type;
	String backupFileKey;
	int batchSize;
	BackupAliasType aliasType;
	
	RestoreDestinationJob restoreJob;
	
	@Before
	public void before() {
		batchSize = 3;
		when(mockConfig.getMaximumBackupBatchSize()).thenReturn(batchSize);
		aliasType = BackupAliasType.TABLE_NAME;
		when(mockConfig.getBackupAliasType()).thenReturn(aliasType);
		type = MigrationType.NODE;
		backupFileKey = "backup file key";
		restoreJob = new RestoreDestinationJob(type, backupFileKey);
		
		when(mockAsynchronousJobExecutor.startDestionationJob(any(AdminRequest.class), any())).thenReturn(mockFuture);
		
		destinationExecutor = new DestinationJobExecutorImpl(mockConfig, mockAsynchronousJobExecutor);
	}
	
	@Test
	public void testStartDestinationJob() {
		RestoreTypeRequest expectedRequest = new RestoreTypeRequest();
		expectedRequest.setAliasType(aliasType);
		expectedRequest.setBatchSize((long) batchSize);
		expectedRequest.setMigrationType(restoreJob.getMigrationType());
		expectedRequest.setBackupFileKey(restoreJob.getBackupFileKey());
		// call under test
		Future future = destinationExecutor.startDestinationJob(restoreJob);
		assertEquals(mockFuture, future);
		verify(mockAsynchronousJobExecutor).startDestionationJob(expectedRequest, RestoreTypeResponse.class);
	}
}
