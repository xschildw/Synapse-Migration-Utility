package org.sagebionetworks.migration.async.checksum;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.sagebionetworks.migration.async.AsynchronousJobExecutor;
import org.sagebionetworks.migration.async.BackupJobExecutor;
import org.sagebionetworks.migration.async.ResultPair;
import org.sagebionetworks.repo.model.migration.MigrationRangeChecksum;

public class ChecksumRangeExecutorTest {

	@Mock
	AsynchronousJobExecutor asynchronousJobExecutor;
	@Mock
	BackupJobExecutor backupJobExecutor;
	
	@Before
	public void before() {
		
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
		boolean isMatch = false;
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
	

	/**
	 * Helper to create a checksum results from both the source and destination.
	 * 
	 * @param isMatch
	 *            Should the checksums match?
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
}
