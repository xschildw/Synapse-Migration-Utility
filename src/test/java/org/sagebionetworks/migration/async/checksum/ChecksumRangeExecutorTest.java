package org.sagebionetworks.migration.async.checksum;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.*;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.async.AsynchronousJobExecutor;
import org.sagebionetworks.migration.async.BackupJobExecutor;
import org.sagebionetworks.migration.async.DeleteDestinationJob;
import org.sagebionetworks.migration.async.DestinationJob;
import org.sagebionetworks.migration.async.ResultPair;
import org.sagebionetworks.repo.model.migration.AsyncMigrationRangeChecksumRequest;
import org.sagebionetworks.repo.model.migration.MigrationRangeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationType;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class ChecksumRangeExecutorTest {

	@Mock
	AsynchronousJobExecutor mockAsynchronousJobExecutor;
	@Mock
	BackupJobExecutor mockBackupJobExecutor;

	MigrationType type;
	Long minimumId;
	Long maximumId;
	String salt;

	List<DestinationJob> jobs;

	@Before
	public void before() {
		type = MigrationType.ACCESS_APPROVAL;
		minimumId = 1L;
		maximumId = 99L;
		salt = "salt";

		// Setup to return two jobs
		DeleteDestinationJob one = new DeleteDestinationJob();
		one.setMigrationType(type);
		one.setRowIdsToDelete(Lists.newArrayList(123L, 456L));
		DeleteDestinationJob two = new DeleteDestinationJob();
		two.setMigrationType(type);
		two.setRowIdsToDelete(Lists.newArrayList(444L));
		jobs = Lists.newArrayList(one, two);
		when(mockBackupJobExecutor.executeBackupJob(any(MigrationType.class), any(Long.class), any(Long.class)))
				.thenReturn(jobs.iterator());

	}

	@Test
	public void testDoChecksumsMatchTrue() {
		boolean isMatch = true;
		ResultPair<MigrationRangeChecksum> resutls = createChecksumPair(isMatch);
		// call under test
		assertTrue(ChecksumRangeExecutor.doChecksumsMatch(resutls));
	}

	@Test
	public void testDoChecksumsMatchFalse() {
		boolean isMatch = false;
		ResultPair<MigrationRangeChecksum> resutls = createChecksumPair(isMatch);
		// call under test
		assertFalse(ChecksumRangeExecutor.doChecksumsMatch(resutls));
	}

	@Test
	public void testDoChecksumsMatchNull() {
		ResultPair<MigrationRangeChecksum> resutls = null;
		// call under test
		assertFalse(ChecksumRangeExecutor.doChecksumsMatch(resutls));
	}

	@Test
	public void testDoChecksumsMatchNullSouce() {
		boolean isMatch = true;
		ResultPair<MigrationRangeChecksum> resutls = createChecksumPair(isMatch);
		resutls.setSourceResult(null);
		// call under test
		assertFalse(ChecksumRangeExecutor.doChecksumsMatch(resutls));
	}

	@Test
	public void testDoChecksumsMatchNullDestination() {
		boolean isMatch = true;
		ResultPair<MigrationRangeChecksum> resutls = createChecksumPair(isMatch);
		resutls.setDestinationResult(null);
		// call under test
		assertFalse(ChecksumRangeExecutor.doChecksumsMatch(resutls));
	}

	@Test
	public void testDoChecksumsMatchNullSouceChecksum() {
		boolean isMatch = true;
		ResultPair<MigrationRangeChecksum> resutls = createChecksumPair(isMatch);
		resutls.getSourceResult().setChecksum(null);
		// call under test
		assertFalse(ChecksumRangeExecutor.doChecksumsMatch(resutls));
	}

	@Test
	public void testDoChecksumsMatchNullDestChecksum() {
		boolean isMatch = true;
		ResultPair<MigrationRangeChecksum> resutls = createChecksumPair(isMatch);
		resutls.getDestinationResult().setChecksum(null);
		// call under test
		assertFalse(ChecksumRangeExecutor.doChecksumsMatch(resutls));
	}

	@Test
	public void testDoChecksumsMatchNullSouceAndDestChecksum() {
		boolean isMatch = true;
		ResultPair<MigrationRangeChecksum> resutls = createChecksumPair(isMatch);
		resutls.getSourceResult().setChecksum(null);
		resutls.getDestinationResult().setChecksum(null);
		// call under test
		assertTrue(ChecksumRangeExecutor.doChecksumsMatch(resutls));
	}

	@Test
	public void testIteratorNoMatch() {
		boolean isMatch = false;
		setupChecksumCheck(isMatch);

		ChecksumRangeExecutor executor = new ChecksumRangeExecutor(mockAsynchronousJobExecutor, mockBackupJobExecutor,
				type, minimumId, maximumId, salt);
		// call under test
		assertTrue(executor.hasNext());
		// call under test
		DestinationJob next = executor.next();
		assertNotNull(next);
		assertEquals(jobs.get(0), next);
		// call under test
		assertTrue(executor.hasNext());
		// call under test
		next = executor.next();
		assertNotNull(next);
		assertEquals(jobs.get(1), next);
		// call under test
		assertFalse(executor.hasNext());

		// Verify checksum request
		AsyncMigrationRangeChecksumRequest expectedRequest = new AsyncMigrationRangeChecksumRequest();
		expectedRequest.setMaxId(this.maximumId);
		expectedRequest.setMinId(this.minimumId);
		expectedRequest.setSalt(this.salt);
		expectedRequest.setMigrationType(this.type);
		verify(mockAsynchronousJobExecutor).executeSourceAndDestinationJob(expectedRequest,
				MigrationRangeChecksum.class);

		// verify backup request
		verify(mockBackupJobExecutor).executeBackupJob(this.type, this.minimumId, this.maximumId + 1L);
	}

	@Test
	public void testIteratorMatch() {
		boolean isMatch = true;
		setupChecksumCheck(isMatch);

		ChecksumRangeExecutor executor = new ChecksumRangeExecutor(mockAsynchronousJobExecutor, mockBackupJobExecutor,
				type, minimumId, maximumId, salt);
		// call under test
		assertFalse(executor.hasNext());
		assertEquals(null, executor.next());

		// Verify checksum request
		AsyncMigrationRangeChecksumRequest expectedRequest = new AsyncMigrationRangeChecksumRequest();
		expectedRequest.setMaxId(this.maximumId);
		expectedRequest.setMinId(this.minimumId);
		expectedRequest.setSalt(this.salt);
		expectedRequest.setMigrationType(this.type);
		verify(mockAsynchronousJobExecutor).executeSourceAndDestinationJob(expectedRequest,
				MigrationRangeChecksum.class);

		// verify backup request
		verify(mockBackupJobExecutor, never()).executeBackupJob(any(MigrationType.class), any(Long.class), any(Long.class));
	}

	/**
	 * Helper to create a checksum results from both the source and destination.
	 * 
	 * @param isMatch Should the checksums match?
	 * @return
	 */
	public ResultPair<MigrationRangeChecksum> createChecksumPair(boolean isMatch) {
		ResultPair<MigrationRangeChecksum> result = new ResultPair<>();
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

	/**
	 * Helper to setup a checksum match
	 * 
	 * @param isMatch
	 */
	public void setupChecksumCheck(boolean isMatch) {
		// setup checksums do not match
		ResultPair<MigrationRangeChecksum> resutls = createChecksumPair(isMatch);
		when(mockAsynchronousJobExecutor.executeSourceAndDestinationJob(any(AsyncMigrationRangeChecksumRequest.class),
				any(Class.class))).thenReturn(resutls);
	}

}
