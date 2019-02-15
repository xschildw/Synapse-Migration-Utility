package org.sagebionetworks.migration.async;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.async.checksum.ChecksumDeltaBuilder;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.util.Clock;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class MigrationDriverImplTest {

	@Mock
	Configuration mockConfig;
	@Mock
	MissingFromDestinationBuilder mockMissingFromDestinationBuilder;
	@Mock
	ChecksumDeltaBuilder mockChecksumChangeBuilder;
	@Mock
	Clock mockClock;
	@Mock
	RestoreJobQueue mockRestoreJobQueue;

	List<TypeToMigrateMetadata> primaryTypes;
	RestoreDestinationJob jobOne;
	RestoreDestinationJob jobTwo;
	RestoreDestinationJob jobThree;
	List<DestinationJob> missingJobs;
	List<DestinationJob> deltaJobs;

	MigrationDriverImpl migrationDriver;

	@Before
	public void before() {
		jobOne = new RestoreDestinationJob(MigrationType.NODE, "someKey1");
		jobTwo = new RestoreDestinationJob(MigrationType.NODE, "someKey2");
		jobThree = new RestoreDestinationJob(MigrationType.NODE, "someKey3");
		missingJobs = Lists.newArrayList(jobOne, jobTwo);
		deltaJobs = Lists.newArrayList(jobThree);

		TypeToMigrateMetadata toMigrate = new TypeToMigrateMetadata();
		toMigrate.setType(MigrationType.NODE);
		toMigrate.setSrcMinId(1L);
		toMigrate.setSrcMaxId(99L);
		toMigrate.setSrcCount(98L);
		toMigrate.setDestMinId(1L);
		toMigrate.setDestMaxId(4L);
		toMigrate.setDestCount(3L);
		primaryTypes = Lists.newArrayList(toMigrate);

		when(mockMissingFromDestinationBuilder.buildDestinationJobs(primaryTypes)).thenReturn(missingJobs.iterator());
		when(mockChecksumChangeBuilder.buildAllRestoreJobsForMismatchedChecksums(primaryTypes))
				.thenReturn(deltaJobs.iterator());
		when(mockRestoreJobQueue.isDone()).thenReturn(false, false, true);

		migrationDriver = new MigrationDriverImpl(mockConfig, mockMissingFromDestinationBuilder,
				mockChecksumChangeBuilder, mockRestoreJobQueue, mockClock);
	}

	@Test
	public void testMigratePrimaryTypes() throws InterruptedException {
		// call under test
		migrationDriver.migratePrimaryTypes(primaryTypes);
		verify(mockMissingFromDestinationBuilder).buildDestinationJobs(primaryTypes);
		verify(mockChecksumChangeBuilder).buildAllRestoreJobsForMismatchedChecksums(primaryTypes);
		// Three jobs should be pushed to the queue
		verify(mockRestoreJobQueue, times(3)).pushJob(any(DestinationJob.class));
		verify(mockRestoreJobQueue).pushJob(jobOne);
		verify(mockRestoreJobQueue).pushJob(jobOne);
		verify(mockRestoreJobQueue).pushJob(jobOne);

		// Should sleep twice waiting for the restore jobs to finish.
		verify(mockClock, times(2)).sleep(MigrationDriverImpl.SLEEP_TIME_MS);
	}

	@Test(expected = RuntimeException.class)
	public void testMigratePrimaryTypesInterupt() throws InterruptedException {
		// Interrupt should become runtime.
		doThrow(new InterruptedException()).when(mockClock).sleep(any(Long.class));
		// call under test
		migrationDriver.migratePrimaryTypes(primaryTypes);
	}

}
