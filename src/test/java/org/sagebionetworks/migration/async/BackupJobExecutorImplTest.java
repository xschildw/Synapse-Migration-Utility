package org.sagebionetworks.migration.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.repo.model.daemon.BackupAliasType;
import org.sagebionetworks.repo.model.migration.AdminRequest;
import org.sagebionetworks.repo.model.migration.BackupTypeRangeRequest;
import org.sagebionetworks.repo.model.migration.BackupTypeResponse;
import org.sagebionetworks.repo.model.migration.CalculateOptimalRangeRequest;
import org.sagebionetworks.repo.model.migration.CalculateOptimalRangeResponse;
import org.sagebionetworks.repo.model.migration.IdRange;
import org.sagebionetworks.repo.model.migration.MigrationType;

import com.google.common.collect.Lists;

@ExtendWith(MockitoExtension.class)
public class BackupJobExecutorImplTest {

	@Mock
	Configuration mockConfiguration;
	@Mock
	AsynchronousJobExecutor mockAsynchronousJobExecutor;

	BackupJobExecutorImpl executor;

	MigrationType migrationType;
	BackupAliasType backupAliasType;
	int batchSize;

	CalculateOptimalRangeResponse rangeResponse;

	BackupTypeResponse backupTypeResponse;

	String backupFile;

	@BeforeEach
	public void before() {
		batchSize = 1;
		lenient().when(mockConfiguration.getMaximumBackupBatchSize()).thenReturn(batchSize);
		backupAliasType = BackupAliasType.TABLE_NAME;
		lenient().when(mockConfiguration.getBackupAliasType()).thenReturn(backupAliasType);
		migrationType = MigrationType.NODE;

		rangeResponse = new CalculateOptimalRangeResponse();
		rangeResponse.setMigrationType(migrationType);
		rangeResponse.setRanges(Lists.newArrayList(createIdRange(3L, 4L)));

		backupTypeResponse = new BackupTypeResponse();
		backupFile = "backupFile";
		backupTypeResponse.setBackupFileKey(backupFile);

		lenient().when(mockAsynchronousJobExecutor.executeSourceJob(any(AdminRequest.class), any())).thenReturn(rangeResponse, backupTypeResponse);

		executor = new BackupJobExecutorImpl(mockConfiguration, mockAsynchronousJobExecutor);
	}

	@Test
	public void testCreateContiguousBackupRangeRequestsSparse() {
		long minimumId = 1L;
		long maximumId = 8L;
		List<IdRange> sparseRange = Lists.newArrayList(createIdRange(2L, 4L), createIdRange(6L, 7L));
		// call under test
		List<BackupTypeRangeRequest> results = BackupJobExecutorImpl.createContiguousBackupRangeRequests(
				backupAliasType, batchSize, migrationType, minimumId, maximumId, sparseRange);
		assertNotNull(results);
		assertEquals(2, results.size());
		// one
		BackupTypeRangeRequest request = results.get(0);
		assertEquals(backupAliasType, request.getAliasType());
		assertEquals(new Long(batchSize), request.getBatchSize());
		assertEquals(migrationType, request.getMigrationType());
		assertEquals(new Long(1), request.getMinimumId());
		assertEquals(new Long(4), request.getMaximumId());
		// two
		request = results.get(1);
		assertEquals(backupAliasType, request.getAliasType());
		assertEquals(new Long(batchSize), request.getBatchSize());
		assertEquals(migrationType, request.getMigrationType());
		assertEquals(new Long(5), request.getMinimumId());
		assertEquals(new Long(8), request.getMaximumId());
	}

	@Test
	public void testCreateContiguousBackupRangeRequestsNoGaps() {
		long minimumId = 1L;
		long maximumId = 8L;
		List<IdRange> sparseRange = Lists.newArrayList(createIdRange(1L, 3L), createIdRange(4L, 5L),
				createIdRange(6L, 8L));
		// call under test
		List<BackupTypeRangeRequest> results = BackupJobExecutorImpl.createContiguousBackupRangeRequests(
				backupAliasType, batchSize, migrationType, minimumId, maximumId, sparseRange);
		assertNotNull(results);
		assertEquals(3, results.size());
		// one
		BackupTypeRangeRequest request = results.get(0);
		assertEquals(backupAliasType, request.getAliasType());
		assertEquals(new Long(batchSize), request.getBatchSize());
		assertEquals(migrationType, request.getMigrationType());
		assertEquals(new Long(1), request.getMinimumId());
		assertEquals(new Long(3), request.getMaximumId());
		// two
		request = results.get(1);
		assertEquals(backupAliasType, request.getAliasType());
		assertEquals(new Long(batchSize), request.getBatchSize());
		assertEquals(migrationType, request.getMigrationType());
		assertEquals(new Long(4), request.getMinimumId());
		assertEquals(new Long(5), request.getMaximumId());
		// three
		request = results.get(2);
		assertEquals(backupAliasType, request.getAliasType());
		assertEquals(new Long(batchSize), request.getBatchSize());
		assertEquals(migrationType, request.getMigrationType());
		assertEquals(new Long(6), request.getMinimumId());
		assertEquals(new Long(8), request.getMaximumId());
	}

	@Test
	public void testCreateContiguousBackupRangeRequestsOneValue() {
		long minimumId = 1L;
		long maximumId = 8L;
		List<IdRange> range = Lists.newArrayList(createIdRange(1L, 8L));
		// call under test
		List<BackupTypeRangeRequest> results = BackupJobExecutorImpl.createContiguousBackupRangeRequests(
				backupAliasType, batchSize, migrationType, minimumId, maximumId, range);
		assertNotNull(results);
		assertEquals(1, results.size());
		// one
		BackupTypeRangeRequest request = results.get(0);
		assertEquals(backupAliasType, request.getAliasType());
		assertEquals(new Long(batchSize), request.getBatchSize());
		assertEquals(migrationType, request.getMigrationType());
		assertEquals(new Long(1), request.getMinimumId());
		assertEquals(new Long(8), request.getMaximumId());
	}

	@Test
	public void testCreateContiguousBackupRangeRequestsEmptyRange() {
		long minimumId = 1L;
		long maximumId = 8L;
		// range is empty if there is no data for this range on the source.
		List<IdRange> range = new LinkedList<>();
		// call under test
		List<BackupTypeRangeRequest> results = BackupJobExecutorImpl.createContiguousBackupRangeRequests(
				backupAliasType, batchSize, migrationType, minimumId, maximumId, range);
		assertNotNull(results);
		assertEquals(1, results.size());
		// one
		BackupTypeRangeRequest request = results.get(0);
		assertEquals(backupAliasType, request.getAliasType());
		assertEquals(new Long(batchSize), request.getBatchSize());
		assertEquals(migrationType, request.getMigrationType());
		assertEquals(new Long(1), request.getMinimumId());
		assertEquals(new Long(8), request.getMaximumId());
	}

	/**
	 * Helper to create a range.
	 *
	 * @param min
	 * @param max
	 * @return
	 */
	public IdRange createIdRange(long min, long max) {
		IdRange range = new IdRange();
		range.setMinimumId(min);
		range.setMaximumId(max);
		return range;
	}

	@Test
	public void testExecuteBackupJob() {
		long minimumId = 1L;
		long maximumId = 8L;
		// call under test
		Iterator<DestinationJob> iterator = executor.executeBackupJob(migrationType, minimumId, maximumId);
		assertNotNull(iterator);

		CalculateOptimalRangeRequest expectedRangeRequset = new CalculateOptimalRangeRequest();
		expectedRangeRequset.setMigrationType(migrationType);
		expectedRangeRequset.setMinimumId(minimumId);
		expectedRangeRequset.setMaximumId(maximumId);
		expectedRangeRequset.setOptimalRowsPerRange((long) batchSize);
		verify(mockAsynchronousJobExecutor).executeSourceJob(expectedRangeRequset, CalculateOptimalRangeResponse.class);

		assertTrue(iterator.hasNext());
		DestinationJob job = iterator.next();
		assertTrue(job instanceof RestoreDestinationJob);
		RestoreDestinationJob restoreJob = (RestoreDestinationJob) job;
		assertEquals(backupFile, restoreJob.getBackupFileKey());
		assertEquals(new Long(minimumId), restoreJob.getMinimumId());
		assertEquals(new Long(maximumId), restoreJob.getMaximumId());
		assertEquals(migrationType, restoreJob.getMigrationType());
		assertFalse(iterator.hasNext());
	}

}
