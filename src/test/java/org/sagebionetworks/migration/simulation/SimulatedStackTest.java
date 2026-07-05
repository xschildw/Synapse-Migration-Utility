package org.sagebionetworks.migration.simulation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.sagebionetworks.repo.model.migration.MigrationType.*;
import static org.sagebionetworks.repo.model.migration.MigrationType.PRINCIPAL;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;
import org.sagebionetworks.repo.model.migration.AsyncMigrationTypeCountsRequest;
import org.sagebionetworks.repo.model.migration.BackupTypeRangeRequest;
import org.sagebionetworks.repo.model.migration.BackupTypeResponse;
import org.sagebionetworks.repo.model.migration.BatchChecksumRequest;
import org.sagebionetworks.repo.model.migration.BatchChecksumResponse;
import org.sagebionetworks.repo.model.migration.CalculateOptimalRangeRequest;
import org.sagebionetworks.repo.model.migration.CalculateOptimalRangeResponse;
import org.sagebionetworks.repo.model.migration.IdRange;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;
import org.sagebionetworks.repo.model.migration.MigrationTypeCounts;
import org.sagebionetworks.repo.model.migration.RangeChecksum;
import org.sagebionetworks.repo.model.migration.RestoreTypeRequest;
import org.sagebionetworks.repo.model.migration.RestoreTypeResponse;
import org.sagebionetworks.repo.model.table.Row;

public class SimulatedStackTest {

	@Test
	public void testExecuteAsyncMigrationTypeCountsRequest() {

		SimulatedStack stack = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(12L).setMaxid(42L),
						new MigrationTypeCount().setType(CHANGE).setMinid(4L).setMaxid(101L)));

		// call under test
		MigrationTypeCounts result = stack.executeAsyncMigrationTypeCountsRequest(
				new AsyncMigrationTypeCountsRequest().setTypes(List.of(PRINCIPAL, CHANGE, ACTIVITY)));

		MigrationTypeCounts expected = new MigrationTypeCounts()
				.setList(List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(12L).setMaxid(42L).setCount(31L),
						new MigrationTypeCount().setType(CHANGE).setMinid(4L).setMaxid(101L).setCount(98L),
						new MigrationTypeCount().setType(ACTIVITY).setCount(0L).setMinid(null).setMaxid(null)));
		assertEquals(expected, result);
	}

	@Test
	public void testExecuteAsyncMigrationTypeCountsRequestWithNonContiguous() {

		SimulatedStack stack = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(1L).setMaxid(5L),
						new MigrationTypeCount().setType(CHANGE).setMinid(2L).setMaxid(8L)));

		stack.addRow(PRINCIPAL, 15L);
		stack.addRow(CHANGE, 11L);
		stack.addRow(CHANGE, 18L);

		// call under test
		MigrationTypeCounts result = stack.executeAsyncMigrationTypeCountsRequest(
				new AsyncMigrationTypeCountsRequest().setTypes(List.of(PRINCIPAL, CHANGE)));
		MigrationTypeCounts expected = new MigrationTypeCounts()
				.setList(List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(1L).setMaxid(15L).setCount(6L),
						new MigrationTypeCount().setType(CHANGE).setMinid(2L).setMaxid(18L).setCount(9L)

				));
		assertEquals(expected, result);
	}
	
	@Test
	public void testExecuteAsyncMigrationTypeCountsRequestWithEmpty() {

		SimulatedStack stack = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(PRINCIPAL).setCount(0L),
						new MigrationTypeCount().setType(CHANGE).setMinid(4L).setMaxid(101L)));

		// call under test
		MigrationTypeCounts result = stack.executeAsyncMigrationTypeCountsRequest(
				new AsyncMigrationTypeCountsRequest().setTypes(List.of(PRINCIPAL, CHANGE)));

		MigrationTypeCounts expected = new MigrationTypeCounts()
				.setList(List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(null).setMaxid(null).setCount(0L),
						new MigrationTypeCount().setType(CHANGE).setMinid(4L).setMaxid(101L).setCount(98L)));
		assertEquals(expected, result);
	}

	@Test
	public void testExecuteCalculateOptimalRangeRequest() {

		SimulatedStack stack = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(2L).setMaxid(45L),
						new MigrationTypeCount().setType(CHANGE).setMinid(2L).setMaxid(8L)));

		// call under test
		CalculateOptimalRangeResponse response = stack
				.executeCalculateOptimalRangeRequest(new CalculateOptimalRangeRequest().setMinimumId(13L)
						.setMaximumId(23L).setMigrationType(PRINCIPAL).setOptimalRowsPerRange(3L));
		CalculateOptimalRangeResponse expected = new CalculateOptimalRangeResponse().setMigrationType(PRINCIPAL)
				.setRanges(List.of(new IdRange().setMinimumId(13L).setMaximumId(16L),
						new IdRange().setMinimumId(17L).setMaximumId(20L),
						new IdRange().setMinimumId(21L).setMaximumId(23L)));
		assertEquals(expected, response);

	}

	@Test
	public void testExecuteCalculateOptimalRangeRequestWithNonContiguous() {

		SimulatedStack stack = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(2L).setMaxid(45L),
						new MigrationTypeCount().setType(CHANGE).setMinid(2L).setMaxid(8L)));

		stack.addRow(PRINCIPAL, 52L);

		// call under test
		CalculateOptimalRangeResponse response = stack
				.executeCalculateOptimalRangeRequest(new CalculateOptimalRangeRequest().setMinimumId(33L)
						.setMaximumId(52L).setMigrationType(PRINCIPAL).setOptimalRowsPerRange(3L));
		CalculateOptimalRangeResponse expected = new CalculateOptimalRangeResponse().setMigrationType(PRINCIPAL)
				.setRanges(List.of(new IdRange().setMinimumId(33L).setMaximumId(36L),
						new IdRange().setMinimumId(37L).setMaximumId(40L),
						new IdRange().setMinimumId(41L).setMaximumId(44L),
						new IdRange().setMinimumId(45L).setMaximumId(52L)));
		assertEquals(expected, response);

	}

	@Test
	public void testExecuteBackupTypeRangeRequest() {

		SimulatedStack stack = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(2L).setMaxid(14L),
						new MigrationTypeCount().setType(CHANGE).setMinid(2L).setMaxid(8L)));

		// call under test
		BackupTypeResponse resonse = stack.executeBackupTypeRangeRequest(
				new BackupTypeRangeRequest().setMigrationType(PRINCIPAL).setMinimumId(4L).setMaximumId(13L));
		assertNotNull(resonse);
		assertNotNull(resonse.getBackupFileKey());
		File backupFile = new File(resonse.getBackupFileKey());
		assertTrue(backupFile.exists());

		List<Row> fromBackup = stack.readAndDeleteBackupFile(new RestoreTypeRequest().setMigrationType(PRINCIPAL)
				.setMinimumRowId(4L).setMaximumRowId(13L).setBackupFileKey(resonse.getBackupFileKey()));

		List<Row> expected = stack.getRowsOfType(PRINCIPAL).stream()
				.filter(r -> r.getRowId() >= 4L && r.getRowId() <= 13L).collect(Collectors.toList());
		assertEquals(expected, fromBackup);

		// the backup file should be deleted.
		assertFalse(backupFile.exists());

	}

	@Test
	public void testExecuteBackupTypeRangeRequestAndExecuteRestoreTypeRequest() {

		SimulatedStack source = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(1L).setMaxid(9L)));

		SimulatedStack destination = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(1L).setMaxid(9L)));

		// These two rows should remain in the destination.
		Row destFrist = destination.getRowsOfType(PRINCIPAL).get(0);
		Row destLast = destination.getRowsOfType(PRINCIPAL).get(8);

		long minId = 2L;
		long maxId = 8L;

		// call under test
		BackupTypeResponse backupResponse = source.executeBackupTypeRangeRequest(
				new BackupTypeRangeRequest().setMigrationType(PRINCIPAL).setMinimumId(minId).setMaximumId(maxId));
		assertNotNull(backupResponse);
		assertNotNull(backupResponse.getBackupFileKey());

		// call under test
		RestoreTypeResponse restoreResponse = destination
				.executeRestoreTypeRequest(new RestoreTypeRequest().setMinimumRowId(minId).setMaximumRowId(maxId)
						.setMigrationType(PRINCIPAL).setBackupFileKey(backupResponse.getBackupFileKey()));
		assertEquals(new RestoreTypeResponse().setRestoredRowCount(7L), restoreResponse);

		List<Row> expectedDestination = new ArrayList<>();
		expectedDestination.add(destFrist);
		expectedDestination.addAll(source.getRowsOfType(PRINCIPAL).stream()
				.filter(r -> r.getRowId() >= minId && r.getRowId() <= maxId).collect(Collectors.toList()));
		expectedDestination.add(destLast);

		assertEquals(expectedDestination, destination.getRowsOfType(PRINCIPAL));

	}

	@Test
	public void testExecuteBatchChecksumRequest() {

		SimulatedStack source = new SimulatedStack(
				List.of(new MigrationTypeCount().setType(PRINCIPAL).setMinid(1L).setMaxid(45L)));

		// call under test
		BatchChecksumResponse response = source.executeBatchChecksumRequest(new BatchChecksumRequest().setBatchSize(8L)
				.setMinimumId(4L).setMaximumId(35L).setMigrationType(PRINCIPAL));
		
		assertEquals(5, response.getCheksums().size());

		BatchChecksumResponse expected = new BatchChecksumResponse().setMigrationType(PRINCIPAL).setCheksums(List.of(
				new RangeChecksum().setBinNumber(0L).setCount(4L).setMinimumId(4L).setMaximumId(7L),
				new RangeChecksum().setBinNumber(1L).setCount(8L).setMinimumId(8L).setMaximumId(15L),
				new RangeChecksum().setBinNumber(2L).setCount(8L).setMinimumId(16L).setMaximumId(23L),
				new RangeChecksum().setBinNumber(3L).setCount(8L).setMinimumId(24L).setMaximumId(31L),
				new RangeChecksum().setBinNumber(4L).setCount(4L).setMinimumId(32L).setMaximumId(35L)
				));
		// the checksums change with each run since the etags are random uuids, so we copy before the compare.
		for(int i=0; i<5; i++) {
			String checksum = response.getCheksums().get(i).getChecksum();
			assertNotNull(checksum);
			expected.getCheksums().get(i).setChecksum(checksum);
		}
		assertEquals(expected, response);
	}
}
