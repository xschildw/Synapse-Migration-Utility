package org.sagebionetworks.migration.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
import org.sagebionetworks.repo.model.migration.MigrationType;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class MissingFromDestinationBuilderImplTest {

	@Mock
	Configuration mockConfig;
	@Mock
	BackupJobExecutor mockBackupJobExecutor;
	
	MissingFromDestinationBuilderImpl builder;
	
	MigrationType type;
	int batchSize;
	String backupFileKey;
	BackupAliasType aliasType;
	
	@Before
	public void before() {
		type = MigrationType.NODE;
		batchSize = 10;
		when(mockConfig.getMaximumBackupBatchSize()).thenReturn(batchSize);
		aliasType = BackupAliasType.TABLE_NAME;
		when(mockConfig.getBackupAliasType()).thenReturn(aliasType);

		List<DestinationJob> batchOne = Lists.newArrayList(
				new RestoreDestinationJob(MigrationType.NODE, "one"),
				new RestoreDestinationJob(MigrationType.NODE, "two")
		);
		List<DestinationJob> batchTwo = Lists.newArrayList(
				new RestoreDestinationJob(MigrationType.ACTIVITY, "three")
		);
		when(mockBackupJobExecutor.executeBackupJob(any(MigrationType.class), anyLong(), anyLong())).thenReturn(
				batchOne.iterator(),
				batchTwo.iterator()
		);
		builder = new MissingFromDestinationBuilderImpl(mockConfig, mockBackupJobExecutor);
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
		assertTrue(iterator.hasNext());
		DestinationJob job = iterator.next();
		assertTrue(job instanceof RestoreDestinationJob);
		RestoreDestinationJob restoreJob = (RestoreDestinationJob) job;
		assertEquals(MigrationType.NODE, restoreJob.getMigrationType());
		assertEquals("one", restoreJob.getBackupFileKey());
		
		assertTrue(iterator.hasNext());
		job = iterator.next();
		assertTrue(job instanceof RestoreDestinationJob);
		restoreJob = (RestoreDestinationJob) job;
		assertEquals(MigrationType.NODE, restoreJob.getMigrationType());
		assertEquals("two", restoreJob.getBackupFileKey());
		
		assertTrue(iterator.hasNext());
		job = iterator.next();
		assertTrue(job instanceof RestoreDestinationJob);
		restoreJob = (RestoreDestinationJob) job;
		assertEquals(MigrationType.ACTIVITY, restoreJob.getMigrationType());
		assertEquals("three", restoreJob.getBackupFileKey());
		
		// done
		assertFalse(iterator.hasNext());
	}

	@Test
	public void testBuildDestinationJobsWithNullMin() {
		TypeToMigrateMetadata one = new TypeToMigrateMetadata();
		one.setType(MigrationType.NODE);
		one.setSrcMinId(1L);
		one.setSrcMaxId(9L);
		one.setDestMinId(null);
		one.setDestMaxId(null);

		TypeToMigrateMetadata two = new TypeToMigrateMetadata();
		two.setType(MigrationType.ACTIVITY);
		two.setSrcMinId(null);
		two.setSrcMaxId(null);
		two.setDestMinId(null);
		two.setDestMaxId(null);

		List<TypeToMigrateMetadata> primaryTypes = Lists.newArrayList(one, two);

		Iterator<DestinationJob> iterator = builder.buildDestinationJobs(primaryTypes);
		assertTrue(iterator.hasNext());
		DestinationJob job = iterator.next();
		assertTrue(job instanceof RestoreDestinationJob);
		RestoreDestinationJob restoreJob = (RestoreDestinationJob) job;
		assertEquals(MigrationType.NODE, restoreJob.getMigrationType());
		assertEquals("one", restoreJob.getBackupFileKey());

		assertTrue(iterator.hasNext());
		job = iterator.next();
		assertTrue(job instanceof RestoreDestinationJob);
		restoreJob = (RestoreDestinationJob) job;
		assertEquals(MigrationType.NODE, restoreJob.getMigrationType());
		assertEquals("two", restoreJob.getBackupFileKey());

		// done
		assertFalse(iterator.hasNext());
	}
}
