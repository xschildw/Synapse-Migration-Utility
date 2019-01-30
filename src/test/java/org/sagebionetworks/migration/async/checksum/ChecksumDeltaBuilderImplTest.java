package org.sagebionetworks.migration.async.checksum;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.async.DestinationJob;
import org.sagebionetworks.migration.async.RestoreDestinationJob;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.repo.model.migration.MigrationType;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class ChecksumDeltaBuilderImplTest {

	@Mock
	TypeChecksumBuilder typeChecksumProvider;

	@InjectMocks
	ChecksumDeltaBuilderImpl builder;
	
	List<TypeToMigrateMetadata> primaryTypes;
	MigrationType type;
	int batchSize;
	String salt;
	
	@Before
	public void testBuid() {
		TypeToMigrateMetadata one  = new TypeToMigrateMetadata();
		one.setType(MigrationType.NODE);
		one.setSrcMinId(0L);
		one.setSrcMaxId(1L);
		
		TypeToMigrateMetadata two  = new TypeToMigrateMetadata();
		two.setType(MigrationType.ACCESS_APPROVAL);
		two.setSrcMinId(0L);
		two.setSrcMaxId(1L);
		primaryTypes = Lists.newArrayList(one, two);
		
		// First type return empty iterator
		when(typeChecksumProvider.buildAllRestoreJobsForType(eq(one), anyString())).thenReturn(new LinkedList<DestinationJob>().iterator());
		
		// Second return two jobs
		List<DestinationJob> backup = new LinkedList<>();
		backup.add(new RestoreDestinationJob(type, "one"));
		backup.add(new RestoreDestinationJob(type, "two"));
		when(typeChecksumProvider.buildAllRestoreJobsForType(eq(two), anyString())).thenReturn(backup.iterator());		
	}
	
	@Test
	public void testBuildChecksumJobs() {
		Iterator<DestinationJob> iterator = builder.buildAllRestoreJobsForMismatchedChecksums(primaryTypes);
		assertNotNull(iterator);
		assertTrue(iterator.hasNext());
		RestoreDestinationJob job = (RestoreDestinationJob) iterator.next();
		assertEquals("one", job.getBackupFileKey());
		assertTrue(iterator.hasNext());
		job = (RestoreDestinationJob) iterator.next();
		assertEquals("two", job.getBackupFileKey());
		assertFalse(iterator.hasNext());
		verify(typeChecksumProvider, times(2)).buildAllRestoreJobsForType(any(TypeToMigrateMetadata.class), any(String.class));
	}
}
