package org.sagebionetworks.migration.async.checksum;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
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
import org.sagebionetworks.migration.async.DestinationJob;
import org.sagebionetworks.migration.async.RestoreDestinationJob;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.repo.model.migration.MigrationType;

@RunWith(MockitoJUnitRunner.class)
public class TypeChecksumProviderImplTest {

	@Mock
	Configuration mockConfiguration;
	@Mock
	RangeCheksumBuilder mockRangeProvider;

	TypeChecksumProviderImpl provider;

	int batchSize;
	MigrationType type;
	Long srcMinId;
	Long srcMaxId;
	Long srcCount;
	Long destMinId;
	Long destMaxId;
	Long destCount;
	String salt;
	TypeToMigrateMetadata primaryType;
	
	List<DestinationJob> one;
	List<DestinationJob> two;
	List<DestinationJob> three;
	List<DestinationJob> four;

	@Before
	public void before() {
		batchSize = 13;
		when(mockConfiguration.getMaximumBackupBatchSize()).thenReturn(batchSize);
		provider = new TypeChecksumProviderImpl(mockConfiguration, mockRangeProvider);
		
		type = MigrationType.ACL;
		srcMinId = 11L;
		srcMaxId = 67L;
		srcCount = 40L;
		destMinId = 15L;
		destMaxId = 99L;
		destCount = 16L;
		primaryType = new TypeToMigrateMetadata(type, srcMinId, srcMaxId, srcCount, destMinId, destMaxId, destCount);

		one = new LinkedList<>();
		one.add(new RestoreDestinationJob(type, "one"));
		one.add(new RestoreDestinationJob(type, "two"));
		two = new LinkedList<>();
		three = new LinkedList<>();
		three.add(new RestoreDestinationJob(type, "three"));
		four = new LinkedList<>();
		
		when(mockRangeProvider.providerRangeCheck(any(MigrationType.class), any(Long.class), any(Long.class),
				anyString())).thenReturn(one.iterator(), two.iterator(), three.iterator(), four.iterator());
	}
	
	@Test
	public void testBuildAllRestoreJobsForType() {		
		primaryType.setSrcMinId(11L);
		primaryType.setSrcMaxId(66L);
		// call under test
		Iterator<DestinationJob> iterator = provider.buildAllRestoreJobsForType(primaryType, salt);
		assertNotNull(iterator);
		verify(mockRangeProvider, times(4)).providerRangeCheck(any(MigrationType.class), any(Long.class), any(Long.class), anyString());
		
		assertTrue(iterator.hasNext());
		assertEquals(one.get(0), iterator.next());
		assertTrue(iterator.hasNext());
		assertEquals(one.get(1), iterator.next());
		assertTrue(iterator.hasNext());
		assertEquals(three.get(0), iterator.next());
		assertFalse(iterator.hasNext());
	}
	
	@Test
	public void testBuildAllRestoreJobsForTypeUnderBatch() {
		primaryType.setSrcMinId(0L);
		primaryType.setSrcMaxId(batchSize-1L);
		
		// call under test
		Iterator<DestinationJob> iterator = provider.buildAllRestoreJobsForType(primaryType, salt);
		assertNotNull(iterator);
		verify(mockRangeProvider, times(1)).providerRangeCheck(any(MigrationType.class), any(Long.class), any(Long.class), anyString());
		verify(mockRangeProvider).providerRangeCheck(type, 0L, 12L, salt);
	}
	
	@Test
	public void testBuildAllRestoreJobsForTypeEqualsBatch() {
		primaryType.setSrcMinId(0L);
		primaryType.setSrcMaxId((long)batchSize);
		// call under test
		Iterator<DestinationJob> iterator = provider.buildAllRestoreJobsForType(primaryType, salt);
		assertNotNull(iterator);
		verify(mockRangeProvider, times(1)).providerRangeCheck(any(MigrationType.class), any(Long.class), any(Long.class), anyString());
		verify(mockRangeProvider).providerRangeCheck(type, 0L, 13L, salt);
	}
	
	@Test
	public void testBuildAllRestoreJobsForTypeOverBatch() {
		primaryType.setSrcMinId(0L);
		primaryType.setSrcMaxId(batchSize+1L);
		// call under test
		Iterator<DestinationJob> iterator = provider.buildAllRestoreJobsForType(primaryType, salt);
		assertNotNull(iterator);
		verify(mockRangeProvider, times(2)).providerRangeCheck(any(MigrationType.class), any(Long.class), any(Long.class), anyString());
		verify(mockRangeProvider).providerRangeCheck(type, 0L, 13L, salt);
		verify(mockRangeProvider).providerRangeCheck(type, 14L, 14L, salt);
	}
	
	@Test
	public void testBuildAllRestoreJobsForTypeOdd() {
		primaryType.setSrcMinId(0L);
		primaryType.setSrcMaxId(batchSize*3L);
		// call under test
		Iterator<DestinationJob> iterator = provider.buildAllRestoreJobsForType(primaryType, salt);
		assertNotNull(iterator);
		verify(mockRangeProvider, times(3)).providerRangeCheck(any(MigrationType.class), any(Long.class), any(Long.class), anyString());
		verify(mockRangeProvider).providerRangeCheck(type, 0L, 13L, salt);
		verify(mockRangeProvider).providerRangeCheck(type, 14L, 27L, salt);
		verify(mockRangeProvider).providerRangeCheck(type, 28L, 39L, salt);
	}
	
	@Test
	public void testBuildAllRestoreJobsForTypeEven() {
		primaryType.setSrcMinId(0L);
		primaryType.setSrcMaxId(batchSize*2L);
		// call under test
		Iterator<DestinationJob> iterator = provider.buildAllRestoreJobsForType(primaryType, salt);
		assertNotNull(iterator);
		verify(mockRangeProvider, times(2)).providerRangeCheck(any(MigrationType.class), any(Long.class), any(Long.class), anyString());
		verify(mockRangeProvider).providerRangeCheck(type, 0L, 13L, salt);
		verify(mockRangeProvider).providerRangeCheck(type, 14L, 26L, salt);
	}

}
