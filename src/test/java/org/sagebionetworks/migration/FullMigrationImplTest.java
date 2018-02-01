package org.sagebionetworks.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;

import static org.mockito.Mockito.*;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.async.ResultPair;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class FullMigrationImplTest {

	@Mock
	LoggerFactory mockLoggerFactory;
	@Mock
	Logger mockLogger;
	@Mock
	StackStatusService mockStackStatusService;
	@Mock
	TypeService mockTypeService;
	@Mock
	Reporter mockTypeReporter;
	@Mock
	AsynchronousMigration mockAsynchronousMigration;
	@Mock
	Configuration mockConfiguration;
	
	List<MigrationType> allCommonTypes;
	List<MigrationType> commonPrimaryTypes;
	ResultPair<List<MigrationTypeCount>> countResultsOne;
	ResultPair<List<MigrationTypeCount>> countResultsTwo;
	
	ResultPair<List<MigrationTypeChecksum>> checksumResutls;

	FullMigrationImpl fullMigration;

	@Before
	public void before() {
		when(mockLoggerFactory.getLogger(any())).thenReturn(mockLogger);
		allCommonTypes = Lists.newArrayList(MigrationType.NODE, MigrationType.NODE_REVISION);
		when(mockTypeService.getAllCommonMigrationTypes()).thenReturn(allCommonTypes);
		commonPrimaryTypes = Lists.newArrayList(MigrationType.NODE);
		when(mockTypeService.getCommonPrimaryMigrationTypes()).thenReturn(commonPrimaryTypes);

		MigrationTypeCount startCount = new MigrationTypeCount();
		startCount.setType(MigrationType.NODE);
		startCount.setCount(0L);
		
		countResultsOne = new ResultPair<List<MigrationTypeCount>>();
		countResultsOne.setDestinationResult(Lists.newArrayList(startCount));
		
		MigrationTypeCount endCount = new MigrationTypeCount();
		endCount.setType(MigrationType.NODE);
		endCount.setCount(100L);
		
		countResultsTwo = new ResultPair<List<MigrationTypeCount>>();
		countResultsTwo.setDestinationResult(Lists.newArrayList(endCount));
		
		when(mockTypeService.getMigrationTypeCounts(anyListOf(MigrationType.class))).thenReturn(countResultsOne, countResultsTwo);
		
		fullMigration = new FullMigrationImpl(mockLoggerFactory, mockStackStatusService, mockTypeService,
				mockTypeReporter, mockAsynchronousMigration, mockConfiguration);
		
		when(mockConfiguration.includeFullTableChecksums()).thenReturn(true);
		when(mockStackStatusService.isSourceReadOnly()).thenReturn(true);
		
		checksumResutls = new ResultPair<List<MigrationTypeChecksum>>();
		when(mockTypeService.getFullTableChecksums(allCommonTypes)).thenReturn(checksumResutls);
	}
	
	@Test
	public void testRunFullMigration() {
		// call under test
		fullMigration.runFullMigration();
		verify(mockTypeService).getAllCommonMigrationTypes();
		verify(mockTypeService).getCommonPrimaryMigrationTypes();
		verify(mockAsynchronousMigration).migratePrimaryTypes(commonPrimaryTypes);
		// called at start and end.
		verify(mockTypeService, times(2)).getMigrationTypeCounts(allCommonTypes);
		verify(mockTypeReporter).runCountDownBeforeStart();
		// start
		verify(mockTypeReporter).reportCountDifferences(countResultsOne);
		// end
		verify(mockTypeReporter).reportCountDifferences(countResultsTwo);
		verify(mockTypeService).getFullTableChecksums(allCommonTypes);
		verify(mockTypeReporter).reportChecksums(checksumResutls);
	}
	
	@Test
	public void testRunFullMigrationNoCheckSum() {
		when(mockConfiguration.includeFullTableChecksums()).thenReturn(false);
		// call under test
		fullMigration.runFullMigration();
		verify(mockTypeService).getAllCommonMigrationTypes();
		verify(mockTypeService).getCommonPrimaryMigrationTypes();
		verify(mockTypeService, never()).getFullTableChecksums(allCommonTypes);
		verify(mockTypeReporter, never()).reportChecksums(checksumResutls);
	}
	
	@Test
	public void testRunFullMigrationIncludeChecksumNotReadOnly() {
		when(mockConfiguration.includeFullTableChecksums()).thenReturn(true);
		when(mockStackStatusService.isSourceReadOnly()).thenReturn(false);
		// call under test
		fullMigration.runFullMigration();
		verify(mockTypeService).getAllCommonMigrationTypes();
		verify(mockTypeService).getCommonPrimaryMigrationTypes();
		verify(mockTypeService, never()).getFullTableChecksums(allCommonTypes);
		verify(mockTypeReporter, never()).reportChecksums(checksumResutls);
	}
}
