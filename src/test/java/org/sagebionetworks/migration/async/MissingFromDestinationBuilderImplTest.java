package org.sagebionetworks.migration.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;
import org.sagebionetworks.repo.model.migration.AdminRequest;
import org.sagebionetworks.repo.model.migration.BackupTypeResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class MissingFromDestinationBuilderImplTest {

	@Mock
	Configuration mockConfig;
	@Mock
	AsynchronousJobExecutor mockAsynchronousJobExecutor;
	
	MissingFromDestinationBuilderImpl builder;
	
	MigrationType type;
	int batchSize;
	BackupTypeResponse responseOne;
	BackupTypeResponse responseTwo;
	BackupTypeResponse responseThree;
	String backupFileKey;
	BackupAliasType aliasType;
	
	@Before
	public void before() {
		type = MigrationType.NODE;
		batchSize = 10;
		when(mockConfig.getMaximumBackupBatchSize()).thenReturn(batchSize);
		aliasType = BackupAliasType.TABLE_NAME;
		when(mockConfig.getBackupAliasType()).thenReturn(aliasType);

		responseOne = new BackupTypeResponse();
		responseOne.setBackupFileKey("one");
		responseTwo = new BackupTypeResponse();
		responseTwo.setBackupFileKey("two");
		responseThree = new BackupTypeResponse();
		responseThree.setBackupFileKey("three");
		when(mockAsynchronousJobExecutor.executeSourceJob(any(AdminRequest.class), any())).thenReturn(responseOne, responseTwo, responseThree);
		builder = new MissingFromDestinationBuilderImpl(mockConfig, mockAsynchronousJobExecutor);
	}
	
	@Test
	public void testBuildDestinationJobs() {
		TypeToMigrateMetadata one = new TypeToMigrateMetadata();
		one.setType(MigrationType.NODE);
		one.setSrcMinId(1L);
		one.setSrcMaxId(9L);
		one.setDestMinId(null);
		one.setDestMaxId(null);
		
		TypeToMigrateMetadata two = new TypeToMigrateMetadata();
		two.setType(MigrationType.ACTIVITY);
		two.setSrcMinId(4L);
		two.setSrcMaxId(7L);
		two.setDestMinId(null);
		two.setDestMaxId(null);
		
		List<TypeToMigrateMetadata> primaryTypes = Lists.newArrayList(one, two);
		
		Iterator<DestinationJob> iterator = builder.buildDestinationJobs(primaryTypes);
		List<DestinationJob> jobs = new LinkedList<>();
		while(iterator.hasNext()) {
			jobs.add(iterator.next());
		}
		assertEquals(2, jobs.size());
		
		DestinationJob job = jobs.get(0);
		assertTrue(job instanceof RestoreDestinationJob);
		RestoreDestinationJob restoreJob = (RestoreDestinationJob) job;
		assertEquals(MigrationType.NODE, restoreJob.getMigrationType());
		assertEquals("one", restoreJob.getBackupFileKey());
		
		job = jobs.get(1);
		assertTrue(job instanceof RestoreDestinationJob);
		restoreJob = (RestoreDestinationJob) job;
		assertEquals(MigrationType.ACTIVITY, restoreJob.getMigrationType());
		assertEquals("two", restoreJob.getBackupFileKey());
	}
}
