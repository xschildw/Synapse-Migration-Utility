package org.sagebionetworks.migration.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

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
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.repo.model.migration.AsyncMigrationRangeChecksumRequest;
import org.sagebionetworks.repo.model.migration.BackupTypeRangeRequest;
import org.sagebionetworks.repo.model.migration.BackupTypeResponse;
import org.sagebionetworks.repo.model.migration.MigrationRangeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationType;

@RunWith(MockitoJUnitRunner.class)
public class TypeChecksumDeltaIteratorTest {

	@Mock
	Configuration mockConfiguration;
	@Mock
	AsynchronousJobExecutor mockAsynchronousJobExecutor;
	@Mock
	BackupJobExecutor mockBackupJobExecutor;

	long minimumId;
	long maxiumumId;
	MigrationType type;
	long batchSize;
	TypeToMigrateMetadata primaryType;
	String salt;

	List<DestinationJob> restoreJobs;

	TypeChecksumDeltaIterator iterator;

	@Before
	public void before() {
		type = MigrationType.NODE;
		batchSize = 3;
		when(mockConfiguration.getMaximumBackupBatchSize()).thenReturn((int) batchSize);
		salt = "someSalt";

		restoreJobs = new LinkedList<>();
		for (int i = 0; i < 10; i++) {
			restoreJobs.add(new RestoreDestinationJob(type, "key" + i));
		}
		when(mockBackupJobExecutor.executeBackupJob(any(MigrationType.class), anyLong(), anyLong())).thenReturn(
				// two results for the first pass
				restoreJobs.subList(0, 2).iterator(), restoreJobs.subList(2, 3).iterator(),
				restoreJobs.subList(3, 4).iterator(), restoreJobs.subList(4, 5).iterator(),
				restoreJobs.subList(6, 7).iterator(), restoreJobs.subList(7, 8).iterator());

		minimumId = 3L;
		maxiumumId = 9L;

		primaryType = new TypeToMigrateMetadata();
		primaryType.setType(type);
		primaryType.setSrcMinId(minimumId);
		primaryType.setSrcMaxId(maxiumumId);

		iterator = new TypeChecksumDeltaIterator(mockConfiguration, mockAsynchronousJobExecutor, mockBackupJobExecutor,
				primaryType, salt);
		// the constructor should not trigger any jobs or calls
		verifyZeroInteractions(mockAsynchronousJobExecutor);
		verifyZeroInteractions(mockBackupJobExecutor);
	}

	@Test
	public void testTypeChecksumDeltaIteratorChecksumsMatch() {
		// setup a match
		boolean isMatch = true;
		ResultPair<AdminResponse> checksumResult = createChecksumPair(isMatch);
		when(mockAsynchronousJobExecutor.executeSourceAndDestinationJob(any(AsyncMigrationRangeChecksumRequest.class),
				any())).thenReturn(checksumResult);
		// when the checksums match there is no work
		assertFalse(iterator.hasNext());
		// single checksum of the entire range.
		AsyncMigrationRangeChecksumRequest expectedChecksumRequest = new AsyncMigrationRangeChecksumRequest();
		expectedChecksumRequest.setMinId(primaryType.getSrcMinId());
		expectedChecksumRequest.setMaxId(primaryType.getSrcMaxId());
		expectedChecksumRequest.setSalt(salt);
		expectedChecksumRequest.setType(type.name());
		verify(mockAsynchronousJobExecutor).executeSourceAndDestinationJob(expectedChecksumRequest,
				MigrationRangeChecksum.class);
		verify(mockAsynchronousJobExecutor, times(1))
				.executeSourceAndDestinationJob(any(AsyncMigrationRangeChecksumRequest.class), any());
		verifyZeroInteractions(mockBackupJobExecutor);
	}

	/**
	 * With batchSize = 3, min = 3 max = 9 and no checksum matches
	 * <ol>
	 * <li>checksum 3 to 9</li>
	 * <li>divide left 3 to 6 right 7 to 9</li>
	 * <li>checksum 3 to 6</li>
	 * <li>divide left 3 to 4 right 5 to 6</li>
	 * <li>checksum 3 to 4</li>
	 * <li>backukp 3 to 5</li>
	 * <li>checksum 5 to 6</li>
	 * <li>backukp 5 to 7</li>
	 * <li>checksum 7 to 9</li>
	 * <li>backukp 7 to 10</li>
	 * </ol>
	 * <ul>
	 * <li>1st hasNext() hits 1. through 6.</li>
	 * <li>2nd hasNext() returns second backup from 6.</li>
	 * <li>3rd hasNext() hits 7. through 8.</li>
	 * <li>4th hasNext() hits 9. through 10.</li>
	 * </ul>
	 */
	@Test
	public void testTypeChecksumDeltaIteratorChecksumsNoMatch() {
		// There are no matches
		boolean isMatch = false;
		ResultPair<AdminResponse> checksumResult = createChecksumPair(isMatch);
		when(mockAsynchronousJobExecutor.executeSourceAndDestinationJob(any(AsyncMigrationRangeChecksumRequest.class),
				any())).thenReturn(checksumResult);
		// 1st hasNext()
		assertTrue(iterator.hasNext());
		// 1.
		AsyncMigrationRangeChecksumRequest expectedChecksumRequest = new AsyncMigrationRangeChecksumRequest();
		expectedChecksumRequest.setMinId(primaryType.getSrcMinId());
		expectedChecksumRequest.setMaxId(primaryType.getSrcMaxId());
		expectedChecksumRequest.setSalt(salt);
		expectedChecksumRequest.setType(type.name());
		verify(mockAsynchronousJobExecutor).executeSourceAndDestinationJob(expectedChecksumRequest,
				MigrationRangeChecksum.class);
		// 3.
		expectedChecksumRequest = new AsyncMigrationRangeChecksumRequest();
		expectedChecksumRequest.setMinId(3L);
		expectedChecksumRequest.setMaxId(6L);
		expectedChecksumRequest.setSalt(salt);
		expectedChecksumRequest.setType(type.name());
		verify(mockAsynchronousJobExecutor).executeSourceAndDestinationJob(expectedChecksumRequest,
				MigrationRangeChecksum.class);
		// 5.
		expectedChecksumRequest = new AsyncMigrationRangeChecksumRequest();
		expectedChecksumRequest.setMinId(3L);
		expectedChecksumRequest.setMaxId(4L);
		expectedChecksumRequest.setSalt(salt);
		expectedChecksumRequest.setType(type.name());
		verify(mockAsynchronousJobExecutor).executeSourceAndDestinationJob(expectedChecksumRequest,
				MigrationRangeChecksum.class);
		// 6.
		verify(mockBackupJobExecutor).executeBackupJob(type, 3L, 5L);
		RestoreDestinationJob job = (RestoreDestinationJob) iterator.next();
		assertEquals("key0", job.backupFileKey);
		// 1st totals
		verify(mockAsynchronousJobExecutor, times(3)).executeSourceAndDestinationJob(any(AdminRequest.class), any());
		verify(mockBackupJobExecutor, times(1)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());

		// 2nd hasNext()
		assertTrue(iterator.hasNext());
		job = (RestoreDestinationJob) iterator.next();
		assertEquals("key1", job.backupFileKey);
		// 2nd totals
		verify(mockAsynchronousJobExecutor, times(3)).executeSourceAndDestinationJob(any(AdminRequest.class), any());
		verify(mockBackupJobExecutor, times(1)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());

		// 3rd hasNext()
		assertTrue(iterator.hasNext());
		job = (RestoreDestinationJob) iterator.next();
		assertEquals("key2", job.backupFileKey);
		// 7.
		expectedChecksumRequest = new AsyncMigrationRangeChecksumRequest();
		expectedChecksumRequest.setMinId(5L);
		expectedChecksumRequest.setMaxId(6L);
		expectedChecksumRequest.setSalt(salt);
		expectedChecksumRequest.setType(type.name());
		verify(mockAsynchronousJobExecutor).executeSourceAndDestinationJob(expectedChecksumRequest,
				MigrationRangeChecksum.class);
		// 8.
		verify(mockBackupJobExecutor).executeBackupJob(type, 5L, 7L);
		// 3rd totals
		verify(mockAsynchronousJobExecutor, times(4)).executeSourceAndDestinationJob(any(AdminRequest.class), any());
		verify(mockBackupJobExecutor, times(2)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());

		// 4th hasNext()
		assertTrue(iterator.hasNext());
		job = (RestoreDestinationJob) iterator.next();
		assertEquals("key3", job.backupFileKey);
		// 9.
		expectedChecksumRequest = new AsyncMigrationRangeChecksumRequest();
		expectedChecksumRequest.setMinId(7L);
		expectedChecksumRequest.setMaxId(9L);
		expectedChecksumRequest.setSalt(salt);
		expectedChecksumRequest.setType(type.name());
		verify(mockAsynchronousJobExecutor).executeSourceAndDestinationJob(expectedChecksumRequest,
				MigrationRangeChecksum.class);
		// 10.
		verify(mockBackupJobExecutor).executeBackupJob(type, 7L, 10L);
		// 4th totals
		verify(mockAsynchronousJobExecutor, times(5)).executeSourceAndDestinationJob(any(AdminRequest.class), any());
		verify(mockBackupJobExecutor, times(3)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());

		// done
		assertFalse(iterator.hasNext());
	}


	/**
	 * Helper to create a checksum results from both the source and destination.
	 * 
	 * @param isMatch
	 *            Should the checksums match?
	 * @return
	 */
	public ResultPair<AdminResponse> createChecksumPair(boolean isMatch) {
		ResultPair<AdminResponse> result = new ResultPair<>();
		String checksum = "checksum";
		MigrationRangeChecksum sourceChecksum = new MigrationRangeChecksum();
		sourceChecksum.setChecksum(checksum);
		result.setSourceResult(sourceChecksum);
		MigrationRangeChecksum destinationChecksum = new MigrationRangeChecksum();
		if (!isMatch) {
			checksum = "different";
		}
		destinationChecksum.setChecksum(checksum);
		result.setDestinationResult(destinationChecksum);
		return result;
	}

}
