package org.sagebionetworks.migration.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata.TypeToMigrateMetadataBuilder;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;

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

	ArgumentCaptor<Long> minIdCaptor = ArgumentCaptor.forClass(Long.class);
	ArgumentCaptor<Long> maxIdCaptor = ArgumentCaptor.forClass(Long.class);
	
	private boolean isSourceReadOnly;

	
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
		isSourceReadOnly = false;
	}
	
	@Test
	public void testBuildDestinationJobs() {
		TypeToMigrateMetadata one = TypeToMigrateMetadata.builder(isSourceReadOnly)
				.setSource(
						new MigrationTypeCount().setType(MigrationType.NODE).setMinid(1L).setMaxid(99L))
				.setDest(new MigrationTypeCount().setType(MigrationType.NODE).setMinid(null).setMaxid(null))
				.build();
		
		TypeToMigrateMetadata two = TypeToMigrateMetadata.builder(isSourceReadOnly)
				.setSource(
						new MigrationTypeCount().setType(MigrationType.ACTIVITY).setMinid(4L).setMaxid(7L))
				.setDest(new MigrationTypeCount().setType(MigrationType.ACTIVITY).setMinid(null).setMaxid(null))
				.build();
		
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
		TypeToMigrateMetadata one = TypeToMigrateMetadata.builder(isSourceReadOnly)
				.setSource(
						new MigrationTypeCount().setType(MigrationType.NODE).setMinid(5L).setMaxid(20L))
				.setDest(new MigrationTypeCount().setType(MigrationType.NODE).setMinid(null).setMaxid(null))
				.build();

		TypeToMigrateMetadata two = TypeToMigrateMetadata.builder(isSourceReadOnly)
				.setSource(
						new MigrationTypeCount().setType(MigrationType.ACTIVITY).setMinid(null).setMaxid(null))
				.setDest(new MigrationTypeCount().setType(MigrationType.ACTIVITY).setMinid(3L).setMaxid(9L))
				.build();

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
		verify(mockBackupJobExecutor, times(2)).executeBackupJob(any(), minIdCaptor.capture(), maxIdCaptor.capture());
		assertEquals(Arrays.asList(5L, 3L), minIdCaptor.getAllValues());
		assertEquals(Arrays.asList(20L, 9L), maxIdCaptor.getAllValues());

		assertFalse(iterator.hasNext());
	}
}
