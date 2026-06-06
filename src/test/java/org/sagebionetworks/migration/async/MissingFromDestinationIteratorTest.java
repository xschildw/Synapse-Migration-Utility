package org.sagebionetworks.migration.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata.TypeToMigrateMetadataBuilder;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;

import com.google.common.collect.Lists;

@ExtendWith(MockitoExtension.class)
public class MissingFromDestinationIteratorTest {

	@Mock
	Configuration mockConfig;
	@Mock
	BackupJobExecutor mockBackupJobExecutor;

	MigrationType type;
	int batchSize;
	String backupFileKey;
	BackupAliasType aliasType;
	RestoreDestinationJob one;
	RestoreDestinationJob two;
	private boolean isSourceReadOnly;

	@BeforeEach
	public void before() {
		type = MigrationType.NODE;
		batchSize = 3;
		aliasType = BackupAliasType.TABLE_NAME;

		one = new RestoreDestinationJob(type, "one");
		List<DestinationJob> batchOne = Lists.newArrayList(
				one
		);
		two = new RestoreDestinationJob(type, "two");
		List<DestinationJob> batchTwo = Lists.newArrayList(
				two
		);
		lenient().when(mockBackupJobExecutor.executeBackupJob(any(MigrationType.class), anyLong(), anyLong())).thenReturn(
				batchOne.iterator(),
				batchTwo.iterator()
		);
		this.isSourceReadOnly = false;
	}

	@Test
	public void testNothingToDo() {
		TypeToMigrateMetadata ranges = TypeToMigrateMetadata.builder(isSourceReadOnly)
				.setSource(new MigrationTypeCount().setType(type).setMinid(1L).setMaxid(99L).setCount(98L))
				.setDest(new MigrationTypeCount().setType(type).setMinid(1L).setMaxid(99L).setCount(98L)).build();

		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
		verifyNoInteractions(mockBackupJobExecutor);
		// nothing to do.
		assertFalse(iterator.hasNext());
		verifyNoInteractions(mockBackupJobExecutor);
	}

	@Test
	public void testDestinationIsNull() {
		TypeToMigrateMetadata ranges = TypeToMigrateMetadata.builder(isSourceReadOnly)
				.setSource(new MigrationTypeCount().setType(type).setMinid(1L).setMaxid(99L).setCount(98L))
				.setDest(new MigrationTypeCount().setType(type).setMinid(null).setMaxid(null).setCount(null)).build();

		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
		assertTrue(iterator.hasNext());
		assertEquals(one, iterator.next());
		assertFalse(iterator.hasNext());
		// should backup the full range for this case
		verify(mockBackupJobExecutor).executeBackupJob(type, 1L, 99L);
		verify(mockBackupJobExecutor, times(1)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());
	}

	@Test
	public void testSourceMinNull() {
		TypeToMigrateMetadata ranges = TypeToMigrateMetadata.builder(isSourceReadOnly)
				.setSource(new MigrationTypeCount().setType(type).setMinid(null).setMaxid(null).setCount(98L))
				.setDest(new MigrationTypeCount().setType(type).setMinid(1L).setMaxid(99L).setCount(98L)).build();

		MissingFromDestinationIterator it = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);

		// call under test
		it.hasNext();

		verify(mockBackupJobExecutor).executeBackupJob(type, 1L, 99L);
	}

	@Test
	public void testSouceMinLessDestinationMin() {
		TypeToMigrateMetadata ranges = TypeToMigrateMetadata.builder(isSourceReadOnly)
				.setSource(new MigrationTypeCount().setType(type).setMinid(1L).setMaxid(99L).setCount(98L))
				.setDest(new MigrationTypeCount().setType(type).setMinid(11L).setMaxid(99L).setCount(88L)).build();

		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
		assertTrue(iterator.hasNext());
		assertEquals(one, iterator.next());
		assertFalse(iterator.hasNext());
		// should backup the full range for this case
		verify(mockBackupJobExecutor).executeBackupJob(type, 1L, 11L);
		verify(mockBackupJobExecutor, times(1)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());
	}


	@Test
	public void testDestinationMinLessSourceMin() {
		TypeToMigrateMetadata ranges = TypeToMigrateMetadata.builder(isSourceReadOnly)
				.setSource(new MigrationTypeCount().setType(type).setMinid(12L).setMaxid(99L).setCount(88L))
				.setDest(new MigrationTypeCount().setType(type).setMinid(2L).setMaxid(99L).setCount(98L)).build();

		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
		assertTrue(iterator.hasNext());
		assertEquals(one, iterator.next());
		assertFalse(iterator.hasNext());
		// should backup the full range for this case
		verify(mockBackupJobExecutor).executeBackupJob(type, 2L, 12L);
		verify(mockBackupJobExecutor, times(1)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());
	}

	@Test
	public void testSouceMaxMoreDestinationMax() {
		TypeToMigrateMetadata ranges = TypeToMigrateMetadata.builder(isSourceReadOnly)
				.setSource(new MigrationTypeCount().setType(type).setMinid(1L).setMaxid(99L).setCount(98L))
				.setDest(new MigrationTypeCount().setType(type).setMinid(1L).setMaxid(89L).setCount(88L)).build();

		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
		assertTrue(iterator.hasNext());
		assertEquals(one, iterator.next());
		assertFalse(iterator.hasNext());
		// should backup the full range for this case
		verify(mockBackupJobExecutor).executeBackupJob(type, 89L, 99L);
		verify(mockBackupJobExecutor, times(1)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());
	}

	@Test
	public void testDestinationMaxMoreSourceMax() {
		TypeToMigrateMetadata ranges = TypeToMigrateMetadata.builder(isSourceReadOnly)
				.setSource(new MigrationTypeCount().setType(type).setMinid(1L).setMaxid(99L).setCount(98L))
				.setDest(new MigrationTypeCount().setType(type).setMinid(1L).setMaxid(102L).setCount(101L)).build();

		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
		assertTrue(iterator.hasNext());
		assertEquals(one, iterator.next());
		assertFalse(iterator.hasNext());
		// should backup the full range for this case
		verify(mockBackupJobExecutor).executeBackupJob(type, 99L, 102L);
		verify(mockBackupJobExecutor, times(1)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());
	}

	@Test
	public void testSourceMinLessDestinationMinAndSourceMaxGreaterDestinationMax() {
		TypeToMigrateMetadata ranges = TypeToMigrateMetadata.builder(isSourceReadOnly)
				.setSource(new MigrationTypeCount().setType(type).setMinid(1L).setMaxid(99L).setCount(98L))
				.setDest(new MigrationTypeCount().setType(type).setMinid(25L).setMaxid(51L).setCount(26L)).build();

		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);
		assertTrue(iterator.hasNext());
		assertEquals(one, iterator.next());
		assertTrue(iterator.hasNext());
		assertEquals(two, iterator.next());
		assertFalse(iterator.hasNext());

		// should backup the full range for this case
		verify(mockBackupJobExecutor).executeBackupJob(type, 1L, 25L);
		verify(mockBackupJobExecutor).executeBackupJob(type, 51L, 99L);
		verify(mockBackupJobExecutor, times(2)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());
	}

	@Test
	public void testSourceMaxLessThanOrEqualDestinationMin() {
		TypeToMigrateMetadata ranges = TypeToMigrateMetadata.builder(isSourceReadOnly)
				.setSource(new MigrationTypeCount().setType(type).setMinid(1L).setMaxid(1L).setCount(1L))
				.setDest(new MigrationTypeCount().setType(type).setMinid(1L).setMaxid(8L).setCount(5L)).build();

		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockBackupJobExecutor, ranges);

		assertTrue(iterator.hasNext());

		verify(mockBackupJobExecutor).executeBackupJob(type, 1L, 8L);
	}

}
