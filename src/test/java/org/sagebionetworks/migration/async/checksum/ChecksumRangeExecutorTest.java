package org.sagebionetworks.migration.async.checksum;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import org.sagebionetworks.migration.async.AsynchronousJobExecutor;
import org.sagebionetworks.migration.async.BackupJobExecutor;
import org.sagebionetworks.migration.async.DeleteDestinationJob;
import org.sagebionetworks.migration.async.DestinationJob;
import org.sagebionetworks.migration.async.ResultPair;
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.repo.model.migration.BatchChecksumRequest;
import org.sagebionetworks.repo.model.migration.BatchChecksumResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.RangeChecksum;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class ChecksumRangeExecutorTest {

	@Mock
	AsynchronousJobExecutor mockAsynchronousJobExecutor;
	@Mock
	BackupJobExecutor mockBackupJobExecutor;

	Long batchSize;
	MigrationType type;
	Long minimumId;
	Long maximumId;
	String salt;

	List<DestinationJob> jobsOne;
	List<DestinationJob> jobsTwo;

	RangeChecksum srcOne;
	RangeChecksum srcTwo;
	RangeChecksum destOne;

	ChecksumRangeExecutor extractor;

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
		jobsOne = Lists.newArrayList(one, two);

		DeleteDestinationJob three = new DeleteDestinationJob();
		three.setMigrationType(type);
		three.setRowIdsToDelete(Lists.newArrayList(555L));
		jobsTwo = Lists.newArrayList(three);

		when(mockBackupJobExecutor.executeBackupJob(any(MigrationType.class), any(Long.class), any(Long.class)))
				.thenReturn(jobsOne.iterator(), jobsTwo.iterator());

		srcOne = new RangeChecksum();
		srcOne.setBinNumber(1L);
		srcOne.setChecksum("c1");
		srcOne.setCount(4l);
		srcOne.setMinimumId(0L);
		srcOne.setMaximumId(3L);
		srcTwo = new RangeChecksum();
		srcTwo.setBinNumber(2L);
		srcTwo.setChecksum("c2");
		srcTwo.setCount(2l);
		srcTwo.setMinimumId(4L);
		srcTwo.setMaximumId(5L);

		BatchChecksumResponse sourceResponse = new BatchChecksumResponse();
		sourceResponse.setCheksums(Lists.newArrayList(srcOne, srcTwo));
		sourceResponse.setMigrationType(MigrationType.FILE_HANDLE);

		// setup two changes in destination
		BatchChecksumResponse destinationResponse = new BatchChecksumResponse();
		RangeChecksum destOne = copy(srcOne);
		destOne.setChecksum("no match");
		RangeChecksum destTwo = copy(srcTwo);
		destTwo.setChecksum("no match two");
		destinationResponse.setCheksums(Lists.newArrayList(destOne, destTwo));
		destinationResponse.setMigrationType(MigrationType.FILE_HANDLE);

		ResultPair<AdminResponse> resultPair = new ResultPair<>();
		resultPair.setSourceResult(sourceResponse);
		resultPair.setDestinationResult(destinationResponse);
		when(mockAsynchronousJobExecutor.executeSourceAndDestinationJob(any(), any())).thenReturn(resultPair);

		extractor = new ChecksumRangeExecutor(mockAsynchronousJobExecutor, mockBackupJobExecutor, batchSize, type,
				minimumId, maximumId, salt);
	}

	@Test
	public void testFindAllMismatchedRangesPerfectMatch() {
		List<RangeChecksum> srcList = Lists.newArrayList(srcOne, srcTwo);
		RangeChecksum destOne = copy(srcOne);
		RangeChecksum destTwo = copy(srcTwo);
		List<RangeChecksum> destList = Lists.newArrayList(destTwo, destOne);
		// Call under test
		List<RangeChecksum> results = ChecksumRangeExecutor.findAllMismatchedRanges(srcList, destList);
		assertNotNull(results);
		assertTrue(results.isEmpty());
	}

	@Test
	public void testFindAllMismatchedRangesChecksumMismatch() {
		List<RangeChecksum> srcList = Lists.newArrayList(srcOne, srcTwo);
		RangeChecksum destOne = copy(srcOne);
		destOne.setChecksum("not the same");
		RangeChecksum destTwo = copy(srcTwo);
		List<RangeChecksum> destList = Lists.newArrayList(destTwo, destOne);
		// Call under test
		List<RangeChecksum> results = ChecksumRangeExecutor.findAllMismatchedRanges(srcList, destList);
		assertNotNull(results);
		assertEquals(1, results.size());
		assertEquals(srcOne, results.get(0));
	}

	@Test
	public void testFindAllMismatchedRangesCountMismatch() {
		List<RangeChecksum> srcList = Lists.newArrayList(srcOne, srcTwo);
		RangeChecksum destOne = copy(srcOne);
		// mismatch in count should trigger mismatch.
		destOne.setCount(0L);
		RangeChecksum destTwo = copy(srcTwo);
		List<RangeChecksum> destList = Lists.newArrayList(destTwo, destOne);
		// Call under test
		List<RangeChecksum> results = ChecksumRangeExecutor.findAllMismatchedRanges(srcList, destList);
		assertNotNull(results);
		assertEquals(1, results.size());
		assertEquals(srcOne, results.get(0));
	}

	@Test
	public void testFindAllMismatchedRangesDestinationNull() {
		List<RangeChecksum> srcList = Lists.newArrayList(srcOne, srcTwo);
		// destination is empty
		List<RangeChecksum> destList = null;
		// Call under test
		List<RangeChecksum> results = ChecksumRangeExecutor.findAllMismatchedRanges(srcList, destList);
		assertNotNull(results);
		assertEquals(2, results.size());
		assertEquals(srcOne, results.get(0));
		assertEquals(srcTwo, results.get(1));
	}

	@Test
	public void testFindAllMismatchedRangesDestinationEmpty() {
		List<RangeChecksum> srcList = Lists.newArrayList(srcOne, srcTwo);
		// destination is empty
		List<RangeChecksum> destList = new LinkedList<>();
		// Call under test
		List<RangeChecksum> results = ChecksumRangeExecutor.findAllMismatchedRanges(srcList, destList);
		assertNotNull(results);
		assertEquals(2, results.size());
		assertEquals(srcOne, results.get(0));
		assertEquals(srcTwo, results.get(1));
	}

	@Test
	public void testFindAllMismatchedRangesSourceEmpty() {
		List<RangeChecksum> srcList = new LinkedList<>();
		// destination is empty
		List<RangeChecksum> destList = Lists.newArrayList(srcOne, srcTwo);
		// Call under test
		List<RangeChecksum> results = ChecksumRangeExecutor.findAllMismatchedRanges(srcList, destList);
		assertNotNull(results);
		assertEquals(2, results.size());
		assertEquals(srcOne, results.get(0));
		assertEquals(srcTwo, results.get(1));
	}

	@Test
	public void testFindAllMismatchedRangesNullSource() {
		List<RangeChecksum> srcList = null;
		// destination is empty
		List<RangeChecksum> destList = Lists.newArrayList(srcOne, srcTwo);
		// Call under test
		List<RangeChecksum> results = ChecksumRangeExecutor.findAllMismatchedRanges(srcList, destList);
		assertNotNull(results);
		assertEquals(2, results.size());
		assertEquals(srcOne, results.get(0));
		assertEquals(srcTwo, results.get(1));
	}

	@Test
	public void testFindAllMismatchedRanges() {
		// call under test
		Iterator<RangeChecksum> result = extractor.findAllMismatchedRanges();
		assertNotNull(result);
		assertTrue(result.hasNext());
		assertEquals(srcOne, result.next());
		assertTrue(result.hasNext());
		assertEquals(srcTwo, result.next());
		assertFalse(result.hasNext());

		BatchChecksumRequest expectedRequest = new BatchChecksumRequest();
		expectedRequest.setMigrationType(this.type);
		expectedRequest.setBatchSize(this.batchSize);
		expectedRequest.setMinimumId(this.minimumId);
		expectedRequest.setMaximumId(this.maximumId);
		expectedRequest.setSalt(this.salt);

		verify(mockAsynchronousJobExecutor).executeSourceAndDestinationJob(expectedRequest,
				BatchChecksumResponse.class);
	}

	@Test
	public void testFindAllMismatchedRangesMinIdNull() {
		when(mockAsynchronousJobExecutor.executeSourceAndDestinationJob(any(), any())).thenThrow(new IllegalArgumentException());
		extractor = new ChecksumRangeExecutor(mockAsynchronousJobExecutor, mockBackupJobExecutor, batchSize, type,
				null, null, salt);
		// call under test
		Iterator<RangeChecksum> it = extractor.findAllMismatchedRanges();
		assertNotNull(it);
		assertFalse(it.hasNext());
		verify(mockAsynchronousJobExecutor, never()).executeSourceAndDestinationJob(any(), eq(BatchChecksumResponse.class));
	}


	@Test
	public void testHasNextAndNext() {
		// calls under test
		assertTrue(extractor.hasNext());
		assertEquals(jobsOne.get(0), extractor.next());
		assertTrue(extractor.hasNext());
		assertEquals(jobsOne.get(1), extractor.next());
		assertTrue(extractor.hasNext());
		assertEquals(jobsTwo.get(0), extractor.next());
		assertFalse(extractor.hasNext());

		verify(mockAsynchronousJobExecutor).executeSourceAndDestinationJob(any(), any());
		verify(mockBackupJobExecutor).executeBackupJob(type, srcOne.getMinimumId(), srcOne.getMaximumId());
		verify(mockBackupJobExecutor).executeBackupJob(type, srcTwo.getMinimumId(), srcTwo.getMaximumId());
	}

	/**
	 * Create a copy of the given object
	 * 
	 * @param toCopy
	 * @return
	 */
	static RangeChecksum copy(RangeChecksum toCopy) {
		RangeChecksum copy = new RangeChecksum();
		copy.setBinNumber(toCopy.getBinNumber());
		copy.setCount(toCopy.getCount());
		copy.setChecksum(toCopy.getChecksum());
		copy.setMaximumId(toCopy.getMaximumId());
		copy.setMinimumId(toCopy.getMinimumId());
		return copy;
	}

}
