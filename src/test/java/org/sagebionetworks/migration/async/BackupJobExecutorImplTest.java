package org.sagebionetworks.migration.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;
import org.sagebionetworks.repo.model.migration.BackupTypeRangeRequest;
import org.sagebionetworks.repo.model.migration.BackupTypeResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;

@RunWith(MockitoJUnitRunner.class)
public class BackupJobExecutorImplTest {

	@Mock
	Configuration mockConfiguration;
	@Mock
	AsynchronousJobExecutor mockAsynchronousJobExecutor;
	BackupTypeResponse response;
	BackupJobExecutorImpl executor;
	
	BackupAliasType backupAliasType;
	int batchSize;
	
	@Before
	public void before() {
		batchSize = 1;
		when(mockConfiguration.getMaximumBackupBatchSize()).thenReturn(batchSize);
		backupAliasType = BackupAliasType.TABLE_NAME;
		when(mockConfiguration.getBackupAliasType()).thenReturn(backupAliasType);
		
		response = new BackupTypeResponse();
		response.setBackupFileKey("backup-file");
		when(mockAsynchronousJobExecutor.executeSourceJob(any(BackupTypeRangeRequest.class), any())).thenReturn(response);
		executor = new BackupJobExecutorImpl(mockConfiguration, mockAsynchronousJobExecutor);
	}
	
	@Test
	public void testExecuteBackupJob() {
		MigrationType type = MigrationType.NODE;
		Long minimumId = 1L;
		Long maximumId = 2L;
		Iterator<DestinationJob> iterator = executor.executeBackupJob(type, minimumId, maximumId);
		assertTrue(iterator.hasNext());
		DestinationJob job = iterator.next();
		assertTrue(job instanceof RestoreDestinationJob);
		RestoreDestinationJob restoreJob = (RestoreDestinationJob) job;
		assertEquals(response.getBackupFileKey(), restoreJob.getBackupFileKey());
		assertEquals(type, restoreJob.getMigrationType());
		assertEquals(minimumId, restoreJob.getMinimumId());
		assertEquals(maximumId, restoreJob.getMaximumId());
		
		// expected request
		BackupTypeRangeRequest expected = new BackupTypeRangeRequest();
		expected.setAliasType(this.backupAliasType);
		expected.setBatchSize((long) batchSize);
		expected.setMigrationType(type);
		expected.setMinimumId(minimumId);
		expected.setMaximumId(maximumId);
		
		verify(mockAsynchronousJobExecutor).executeSourceJob(expected, BackupTypeResponse.class);
	}
	
}
