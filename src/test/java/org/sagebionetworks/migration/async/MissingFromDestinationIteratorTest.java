package org.sagebionetworks.migration.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.*;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;
import org.sagebionetworks.repo.model.migration.BackupTypeRangeRequest;
import org.sagebionetworks.repo.model.migration.MigrationType;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class MissingFromDestinationIteratorTest {

	@Mock
	Configuration mockConfig;
	@Mock
	BackupJobExecutor mockBackupJobExecutor;
	
	MigrationType type;
	int batchSize;
	String backupFileKey;
	BackupAliasType aliasType;
	long destinationRowCountToIgnore;
	RestoreDestinationJob one;
	RestoreDestinationJob two;
	
	@Before
	public void before() {
		type = MigrationType.NODE;
		batchSize = 3;
		destinationRowCountToIgnore = 10;
		when(mockConfig.getDestinationRowCountToIgnore()).thenReturn(destinationRowCountToIgnore);
		when(mockConfig.getMaximumBackupBatchSize()).thenReturn(batchSize);
		aliasType = BackupAliasType.TABLE_NAME;
		when(mockConfig.getBackupAliasType()).thenReturn(aliasType);

		one = new RestoreDestinationJob(type, "one");
		List<DestinationJob> batchOne = Lists.newArrayList(
				one
		);
		two = new RestoreDestinationJob(type, "two");
		List<DestinationJob> batchTwo = Lists.newArrayList(
				two
		);
		when(mockBackupJobExecutor.executeBackupJob(any(MigrationType.class), anyLong(), anyLong())).thenReturn(
				batchOne.iterator(),
				batchTwo.iterator()
		);
	}
	
	@Test
	public void testNothingToDo() {
		TypeToMigrateMetadata ranges = new TypeToMigrateMetadata();
		ranges.setType(type);
		ranges.setSrcMinId(1L);
		ranges.setSrcMaxId(99L);
		ranges.setSrcCount(98L);
		ranges.setDestMinId(1L);
		ranges.setDestMaxId(99L);
		ranges.setDestCount(98L);
		
		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
		verifyZeroInteractions(mockBackupJobExecutor);
		// nothing to do.
		assertFalse(iterator.hasNext());
		verifyZeroInteractions(mockBackupJobExecutor);
	}
	
	/**
	 * Treat the destination as empty when there are only a few rows
	 * in the destination.
	 */
	@Test
	public void testDestinationCountLessThanIgnore() {
		TypeToMigrateMetadata ranges = new TypeToMigrateMetadata();
		ranges.setType(type);
		ranges.setSrcMinId(1L);
		ranges.setSrcMaxId(99L);
		ranges.setSrcCount(98L);
		ranges.setDestMinId(1L);
		ranges.setDestMaxId(99L);
		ranges.setDestCount(destinationRowCountToIgnore-1);
		
		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
		assertTrue(iterator.hasNext());
		assertEquals(one, iterator.next());
		assertFalse(iterator.hasNext());
		
		// should backup the full range for this case
		verify(mockBackupJobExecutor).executeBackupJob(type, 1L, 100L);
		verify(mockBackupJobExecutor, times(1)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());
	}
	
	@Test
	public void testDestinationIsNull() {
		TypeToMigrateMetadata ranges = new TypeToMigrateMetadata();
		ranges.setType(type);
		ranges.setSrcMinId(1L);
		ranges.setSrcMaxId(99L);
		ranges.setSrcCount(98L);
		ranges.setDestMinId(null);
		ranges.setDestMaxId(null);
		ranges.setDestCount(null);
		
		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
		assertTrue(iterator.hasNext());
		assertEquals(one, iterator.next());
		assertFalse(iterator.hasNext());
		// should backup the full range for this case
		verify(mockBackupJobExecutor).executeBackupJob(type, 1L, 100L);
		verify(mockBackupJobExecutor, times(1)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testSouceMinNull() {
		TypeToMigrateMetadata ranges = new TypeToMigrateMetadata();
		ranges.setType(type);
		ranges.setSrcMinId(null);
		ranges.setSrcMaxId(99L);
		ranges.setSrcCount(98L);
		ranges.setDestMinId(1L);
		ranges.setDestMaxId(99L);
		ranges.setDestCount(98L);
		
		// call under test
		new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testSouceMaxNull() {
		TypeToMigrateMetadata ranges = new TypeToMigrateMetadata();
		ranges.setType(type);
		ranges.setSrcMinId(1L);
		ranges.setSrcMaxId(null);
		ranges.setSrcCount(98L);
		ranges.setDestMinId(1L);
		ranges.setDestMaxId(99L);
		ranges.setDestCount(98L);
		
		// call under test
		new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
	}
	
	@Test
	public void testSouceMinLessDestinationMin() {
		TypeToMigrateMetadata ranges = new TypeToMigrateMetadata();
		ranges.setType(type);
		ranges.setSrcMinId(1L);
		ranges.setSrcMaxId(99L);
		ranges.setSrcCount(98L);
		ranges.setDestMinId(11L);
		ranges.setDestMaxId(99L);
		ranges.setDestCount(88L);
		
		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
		assertTrue(iterator.hasNext());
		assertEquals(one, iterator.next());
		assertFalse(iterator.hasNext());
		// should backup the full range for this case
		verify(mockBackupJobExecutor).executeBackupJob(type, 1L, 12L);
		verify(mockBackupJobExecutor, times(1)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());
	}
	
	
	@Test
	public void testDestinationMinLessSourceMin() {
		TypeToMigrateMetadata ranges = new TypeToMigrateMetadata();
		ranges.setType(type);
		ranges.setSrcMinId(12L);
		ranges.setSrcMaxId(99L);
		ranges.setSrcCount(88L);
		ranges.setDestMinId(2L);
		ranges.setDestMaxId(99L);
		ranges.setDestCount(98L);
		
		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
		assertTrue(iterator.hasNext());
		assertEquals(one, iterator.next());
		assertFalse(iterator.hasNext());
		// should backup the full range for this case
		verify(mockBackupJobExecutor).executeBackupJob(type, 2L, 13L);
		verify(mockBackupJobExecutor, times(1)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());
	}
	
	@Test
	public void testSouceMaxMoreDestinationMax() {
		TypeToMigrateMetadata ranges = new TypeToMigrateMetadata();
		ranges.setType(type);
		ranges.setSrcMinId(1L);
		ranges.setSrcMaxId(99L);
		ranges.setSrcCount(98L);
		ranges.setDestMinId(1L);
		ranges.setDestMaxId(89L);
		ranges.setDestCount(88L);
		
		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
		assertTrue(iterator.hasNext());
		assertEquals(one, iterator.next());
		assertFalse(iterator.hasNext());
		// should backup the full range for this case
		verify(mockBackupJobExecutor).executeBackupJob(type, 89L, 100L);
		verify(mockBackupJobExecutor, times(1)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());
	}
	
	@Test
	public void testDestinationMaxMoreSourceMax() {
		TypeToMigrateMetadata ranges = new TypeToMigrateMetadata();
		ranges.setType(type);
		ranges.setSrcMinId(1L);
		ranges.setSrcMaxId(99L);
		ranges.setSrcCount(98L);
		ranges.setDestMinId(1L);
		ranges.setDestMaxId(102L);
		ranges.setDestCount(88L);
		
		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
		assertTrue(iterator.hasNext());
		assertEquals(one, iterator.next());
		assertFalse(iterator.hasNext());
		// should backup the full range for this case
		verify(mockBackupJobExecutor).executeBackupJob(type, 99L, 103L);
		verify(mockBackupJobExecutor, times(1)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());
	}
	
	@Test
	public void testSourceMinLessDestinationMinAndSourceMaxGreaterDestinationMax() {
		TypeToMigrateMetadata ranges = new TypeToMigrateMetadata();
		ranges.setType(type);
		ranges.setSrcMinId(1L);
		ranges.setSrcMaxId(99L);
		ranges.setSrcCount(98L);
		ranges.setDestMinId(25L);
		ranges.setDestMaxId(51L);
		ranges.setDestCount(26L);
		
		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
		assertTrue(iterator.hasNext());
		assertEquals(one, iterator.next());
		assertTrue(iterator.hasNext());
		assertEquals(two, iterator.next());
		assertFalse(iterator.hasNext());
		
		// should backup the full range for this case
		verify(mockBackupJobExecutor).executeBackupJob(type, 1L, 26L);
		verify(mockBackupJobExecutor).executeBackupJob(type, 51L, 100L);
		verify(mockBackupJobExecutor, times(2)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());
	}
	
}
