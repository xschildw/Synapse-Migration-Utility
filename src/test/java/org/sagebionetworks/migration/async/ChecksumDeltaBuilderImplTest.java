package org.sagebionetworks.migration.async;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
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
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.repo.model.migration.AdminRequest;
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.repo.model.migration.AsyncMigrationRangeChecksumRequest;
import org.sagebionetworks.repo.model.migration.MigrationRangeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationType;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class ChecksumDeltaBuilderImplTest {

	@Mock
	Configuration mockConfiguration;
	@Mock
	AsynchronousJobExecutor mockAsynchronousJobExecutor;
	@Mock
	BackupJobExecutor mockBackupJobExecutor;
	
	ChecksumDeltaBuilderImpl builder;
	
	List<TypeToMigrateMetadata> primaryTypes;
	MigrationType type;
	int batchSize;
	String salt;
	
	@Before
	public void testBuid() {
		TypeToMigrateMetadata one  = new TypeToMigrateMetadata();
		one.setType(MigrationType.NODE);
		one.setSrcMinId(0L);
		one.setSrcMaxId(1L);
		
		TypeToMigrateMetadata two  = new TypeToMigrateMetadata();
		two.setType(MigrationType.ACCESS_APPROVAL);
		two.setSrcMinId(0L);
		two.setSrcMaxId(1L);
		primaryTypes = Lists.newArrayList(one, two);
		
		batchSize = 3;
		when(mockConfiguration.getMaximumBackupBatchSize()).thenReturn((int) batchSize);
		salt = "someSalt";

		List<DestinationJob> backupOne = new LinkedList<>();
		backupOne.add(new RestoreDestinationJob(type, "one"));
		List<DestinationJob> backupTwo = new LinkedList<>();
		backupTwo.add(new RestoreDestinationJob(type, "two"));
		when(mockBackupJobExecutor.executeBackupJob(any(MigrationType.class), anyLong(), anyLong())).thenReturn(backupOne.iterator(), backupTwo.iterator());
		
		// no checksum match
		ResultPair<AdminResponse> checksumResults = new ResultPair<>();
		String checksum = "checksum";
		MigrationRangeChecksum sourceChecksum = new MigrationRangeChecksum();
		sourceChecksum.setChecksum(checksum);
		checksumResults.setSourceResult(sourceChecksum);
		MigrationRangeChecksum destinationChecksum = new MigrationRangeChecksum();
		destinationChecksum.setChecksum("no match");
		checksumResults.setDestinationResult(destinationChecksum);
		when(mockAsynchronousJobExecutor.executeSourceAndDestinationJob(any(AsyncMigrationRangeChecksumRequest.class),
				any())).thenReturn(checksumResults);
		
		builder = new ChecksumDeltaBuilderImpl(mockConfiguration, mockAsynchronousJobExecutor, mockBackupJobExecutor);
	}
	
	@Test
	public void testBuildChecksumJobs() {
		Iterator<DestinationJob> iterator = builder.buildChecksumJobs(primaryTypes);
		assertNotNull(iterator);
		assertTrue(iterator.hasNext());
		RestoreDestinationJob job = (RestoreDestinationJob) iterator.next();
		assertEquals("one", job.getBackupFileKey());
		assertTrue(iterator.hasNext());
		job = (RestoreDestinationJob) iterator.next();
		assertEquals("two", job.getBackupFileKey());
		assertFalse(iterator.hasNext());
		verify(mockAsynchronousJobExecutor, times(2)).executeSourceAndDestinationJob(any(AdminRequest.class), any());
		verify(mockBackupJobExecutor, times(2)).executeBackupJob(any(MigrationType.class), anyLong(), anyLong());
	}
}
