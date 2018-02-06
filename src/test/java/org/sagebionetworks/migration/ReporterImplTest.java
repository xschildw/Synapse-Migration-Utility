package org.sagebionetworks.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Date;
import java.util.List;

import static org.mockito.Mockito.*;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.async.JobTarget;
import org.sagebionetworks.migration.async.ResultPair;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.repo.model.asynch.AsynchJobState;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.AsyncMigrationRequest;
import org.sagebionetworks.repo.model.migration.AsyncMigrationTypeCountsRequest;
import org.sagebionetworks.repo.model.migration.DeleteListRequest;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.sagebionetworks.schema.adapter.org.json.EntityFactory;
import org.sagebionetworks.util.Clock;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class ReporterImplTest {
	
	@Mock
	Configuration mockConfig;
	@Mock
	LoggerFactory mockLoggerFactory;
	@Mock
	Logger mockLogger;
	@Mock
	Clock mockClock;
	
	long delayMS;
	
	List<MigrationTypeCount> sourceCounts;
	List<MigrationTypeCount> destinationCounts;
	ResultPair<List<MigrationTypeCount>> typeCounts;
	
	AsyncMigrationRequest asyncMigrationRequest;
	JobTarget jobTarget;
	AsynchronousJobStatus jobStatus;
	
	ReporterImpl reporter;
	
	MigrationType type;

	@Before
	public void before() {
		when(mockLoggerFactory.getLogger(any())).thenReturn(mockLogger);
		long delayMS = 4000L;
		when(mockConfig.getDelayBeforeMigrationStartMS()).thenReturn(delayMS);
		
		MigrationTypeCount sourceNodeCount = new MigrationTypeCount();
		sourceNodeCount.setType(MigrationType.NODE);
		sourceNodeCount.setCount(99L);
		
		MigrationTypeCount sourceAclCount = new MigrationTypeCount();
		sourceAclCount.setType(MigrationType.ACL);
		sourceAclCount.setCount(88L);
		
		sourceCounts = Lists.newArrayList(sourceNodeCount, sourceAclCount);
		
		
		MigrationTypeCount destNodeCount = new MigrationTypeCount();
		destNodeCount.setType(MigrationType.NODE);
		destNodeCount.setCount(2L);
		
		MigrationTypeCount destActivity = new MigrationTypeCount();
		destActivity.setType(MigrationType.ACTIVITY);
		destActivity.setCount(4L);
		
		destinationCounts = Lists.newArrayList(destNodeCount, destActivity);
		
		typeCounts = new ResultPair<>();
		typeCounts.setSourceResult(sourceCounts);
		typeCounts.setDestinationResult(destinationCounts);
		
		DeleteListRequest deleteRequest = new DeleteListRequest();
		deleteRequest.setMigrationType(MigrationType.NODE);
		
		asyncMigrationRequest = new AsyncMigrationRequest();
		asyncMigrationRequest.setAdminRequest(deleteRequest);
		jobTarget = JobTarget.SOURCE;
		jobStatus = new AsynchronousJobStatus();
		jobStatus.setJobId("123");
		jobStatus.setStartedOn(new Date(1517688868123L));
		jobStatus.setRequestBody(asyncMigrationRequest);
		jobStatus.setJobState(AsynchJobState.PROCESSING);
		type = MigrationType.NODE;
		reporter = new ReporterImpl(mockConfig, mockLoggerFactory, mockClock);
	}
	
	@Test
	public void testRunCountDownBeforeStart() throws InterruptedException {
		// call under test
		reporter.runCountDownBeforeStart();
		// should sleep 4 times
		verify(mockClock, times(4)).sleep(ReporterImpl.ONE_SECOND_MS);
		// message format:
		assertEquals("Migration will start in 4 seconds...", String.format(ReporterImpl.COUNTDOWN_FORMAT, 4L));
		// count down
		verify(mockLogger).info(String.format(ReporterImpl.COUNTDOWN_FORMAT, 4L));
		verify(mockLogger).info(String.format(ReporterImpl.COUNTDOWN_FORMAT, 3L));
		verify(mockLogger).info(String.format(ReporterImpl.COUNTDOWN_FORMAT, 2L));
		verify(mockLogger).info(String.format(ReporterImpl.COUNTDOWN_FORMAT, 1L));
		verify(mockLogger).info(ReporterImpl.STARTING_MIGRATION);
	}
	
	@Test (expected=AsyncMigrationException.class)
	public void testRunCountDownBeforeStartInterupt() throws InterruptedException {
		InterruptedException interrupted = new InterruptedException("Interrupted");
		doThrow(interrupted).when(mockClock).sleep(anyLong());
		// call under test 
		reporter.runCountDownBeforeStart();
	}
	
	@Test
	public void testreportCountDifferences() {
		// call under test
		reporter.reportCountDifferences(typeCounts);
		verify(mockLogger).info("\tNODE:\t-97\t99\t2");
		verify(mockLogger).info("\tACTIVITY\tNA\t4");
		verify(mockLogger, times(2)).info(anyString());
	}
	
	@Test
	public void testFormatElapseMS() {
		long hoursMS = 13 *60*60*1000;
		long minMS = 49*60*1000;
		long secMS = 59 * 1000;
		long ms = 456;
		long total = hoursMS+minMS+ secMS+ms;
		// call under test
		String result = ReporterImpl.formatElapse(total);
		assertEquals("13:49:59.456", result);
	}
	
	@Test
	public void testFormatElapseMSPadded() {
		long hoursMS = 1 *60*60*1000;
		long minMS = 2*60*1000;
		long secMS = 3 * 1000;
		long ms = 4;
		long total = hoursMS+minMS+ secMS+ms;
		// call under test
		String result = ReporterImpl.formatElapse(total);
		assertEquals("01:02:03.004", result);
	}
	
	@Test
	public void testFormatElapseMSLow() {
		// call under test
		String result = ReporterImpl.formatElapse(8000L);
		assertEquals("00:00:08.000", result);
	}
	
	@Test
	public void testJson() throws JSONObjectAdapterException {
		String json = EntityFactory.createJSONStringForEntity(jobStatus);
		System.out.println(json);
		AsynchronousJobStatus clone = EntityFactory.createEntityFromJSONString(json, AsynchronousJobStatus.class);
		assertEquals(clone, jobStatus);
	}
	
	@Test
	public void testReportProgressHasMigrationType() {
		// request with a type.
		DeleteListRequest deleteRequest = new DeleteListRequest();
		deleteRequest.setMigrationType(MigrationType.NODE);
		asyncMigrationRequest = new AsyncMigrationRequest();
		asyncMigrationRequest.setAdminRequest(deleteRequest);
		
		jobStatus.setStartedOn(new Date(1517773464652L));
		long elapseMS = 2545L;
		when(mockClock.currentTimeMillis()).thenReturn(jobStatus.getStartedOn().getTime()+elapseMS);
		// call under test
		reporter.reportProgress(jobTarget, jobStatus);
		verify(mockLogger).info("NODE job: 123 state: PROCESSING on: SOURCE type: 'DeleteListRequest' elapse: 00:00:02.545");
	}
	
	@Test
	public void testReportProgress() {
		// request without a type
		asyncMigrationRequest.setAdminRequest(new AsyncMigrationTypeCountsRequest());
		jobStatus.setStartedOn(new Date(1517773464652L));
		long elapseMS = 51234;
		when(mockClock.currentTimeMillis()).thenReturn(jobStatus.getStartedOn().getTime()+elapseMS);
		// call under test
		reporter.reportProgress(jobTarget, jobStatus);
		verify(mockLogger).info(" job: 123 state: PROCESSING on: SOURCE type: 'AsyncMigrationTypeCountsRequest' elapse: 00:00:51.234");
	}
	
}
