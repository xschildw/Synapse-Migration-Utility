package org.sagebionetworks.migration.async.checksum;

import com.amazonaws.services.dynamodbv2.xspec.M;
import com.google.common.collect.Lists;
import com.sun.xml.internal.org.jvnet.mimepull.MIMEConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.async.BackupJobExecutor;
import org.sagebionetworks.migration.async.DestinationJob;
import org.sagebionetworks.migration.async.MissingFromDestinationBuilderImpl;
import org.sagebionetworks.migration.async.RestoreDestinationJob;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;
import org.sagebionetworks.repo.model.migration.BatchChecksumRequest;
import org.sagebionetworks.repo.model.migration.MigrationType;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ChecksumDeltaBuilderImplTest {
	@Mock
	RangeCheksumBuilder mockRangeProvider;

	ChecksumDeltaBuilderImpl builder;

	MigrationType type;
	String backupFileKey;

	@Before
	public void before() throws Exception {
		builder = new ChecksumDeltaBuilderImpl(mockRangeProvider);
	}

	@After
	public void after() throws Exception {
	}

	@Test
	public void testBuildAllRestoreJobsForMismatchedChecksums() {
		List<DestinationJob> expectedJobs = Lists.newArrayList(
				new RestoreDestinationJob(MigrationType.NODE, "one"),
				new RestoreDestinationJob(MigrationType.ACTIVITY, "two")
		);
		when(mockRangeProvider.providerRangeCheck(any(MigrationType.class), anyLong(), anyLong(), anyString())).thenReturn(expectedJobs.iterator());

		TypeToMigrateMetadata typeToMigrate1 = new TypeToMigrateMetadata();
		typeToMigrate1.setType(MigrationType.NODE);
		typeToMigrate1.setSrcCount(10L);
		typeToMigrate1.setSrcMinId(0L);
		typeToMigrate1.setSrcMaxId(100L);
		TypeToMigrateMetadata typeToMigrate2 = new TypeToMigrateMetadata();
		typeToMigrate2.setType(MigrationType.ACTIVITY);
		typeToMigrate2.setSrcCount(10L);
		typeToMigrate2.setSrcMinId(0L);
		typeToMigrate2.setSrcMaxId(100L);

		List<TypeToMigrateMetadata> primaryTypes = Lists.newArrayList(
				typeToMigrate1,
				typeToMigrate2
		);

		// call under test
		Iterator<DestinationJob> iterator = builder.buildAllRestoreJobsForMismatchedChecksums(primaryTypes);

		assertNotNull(iterator);
		assertTrue(iterator.hasNext());
		DestinationJob job = iterator.next();
		assertTrue(job instanceof RestoreDestinationJob);
		RestoreDestinationJob restoreJob = (RestoreDestinationJob)job;
		assertEquals(MigrationType.NODE, restoreJob.getMigrationType());
		assertEquals("one", restoreJob.getBackupFileKey());
		assertTrue(iterator.hasNext());
		job = iterator.next();
		assertTrue(job instanceof RestoreDestinationJob);
		restoreJob = (RestoreDestinationJob)job;
		assertEquals(MigrationType.ACTIVITY, restoreJob.getMigrationType());
		assertEquals("two", restoreJob.getBackupFileKey());
		assertFalse(iterator.hasNext());

	}

	@Test
	public void testBuildAllRestoreJobsForMismatchedChecksumsException() {
		List<DestinationJob> expectedJobs = Lists.newArrayList(
				new RestoreDestinationJob(MigrationType.NODE, "one")
		);

		TypeToMigrateMetadata typeToMigrate1 = new TypeToMigrateMetadata();
		typeToMigrate1.setType(MigrationType.NODE);
		typeToMigrate1.setSrcCount(10L);
		typeToMigrate1.setSrcMinId(0L);
		typeToMigrate1.setSrcMaxId(100L);
		TypeToMigrateMetadata typeToMigrate2 = new TypeToMigrateMetadata();
		typeToMigrate2.setType(MigrationType.ACTIVITY);
		List<TypeToMigrateMetadata> primaryTypes = Lists.newArrayList(
				typeToMigrate1,
				typeToMigrate2
		);
		when(mockRangeProvider.providerRangeCheck(eq(MigrationType.NODE), anyLong(), anyLong(), anyString())).thenReturn(expectedJobs.iterator());
		when(mockRangeProvider.providerRangeCheck(eq(MigrationType.ACTIVITY), anyLong(), anyLong(), anyString())).thenThrow(new IllegalArgumentException("IllegalArgument"));

		// call under test
		Iterator<DestinationJob> iterator = builder.buildAllRestoreJobsForMismatchedChecksums(primaryTypes);

		assertNotNull(iterator);
		assertTrue(iterator.hasNext());
		DestinationJob job = iterator.next();
		assertTrue(job instanceof RestoreDestinationJob);
		RestoreDestinationJob restoreJob = (RestoreDestinationJob)job;
		assertEquals(MigrationType.NODE, restoreJob.getMigrationType());
		assertEquals("one", restoreJob.getBackupFileKey());
		assertFalse(iterator.hasNext());
	}
}