package org.sagebionetworks.migration.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

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
import org.sagebionetworks.repo.model.migration.BackupTypeRangeRequest;
import org.sagebionetworks.repo.model.migration.BackupTypeResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;

@RunWith(MockitoJUnitRunner.class)
public class MissingFromDestinationIteratorTest {

	@Mock
	Configuration mockConfig;
	@Mock
	AsynchronousJobExecutor mockAsynchronousJobExecutor;
	
	MigrationType type;
	int batchSize;
	BackupTypeResponse responseOne;
	BackupTypeResponse responseTwo;
	BackupTypeResponse responseThree;
	String backupFileKey;
	BackupAliasType aliasType;
	long destinationRowCountToIgnore;
	
	@Before
	public void before() {
		type = MigrationType.NODE;
		batchSize = 3;
		destinationRowCountToIgnore = 1;
		when(mockConfig.getMaximumBackupBatchSize()).thenReturn(batchSize);
		aliasType = BackupAliasType.TABLE_NAME;
		when(mockConfig.getBackupAliasType()).thenReturn(aliasType);

		responseOne = new BackupTypeResponse();
		responseOne.setBackupFileKey("one");
		responseTwo = new BackupTypeResponse();
		responseTwo.setBackupFileKey("two");
		responseThree = new BackupTypeResponse();
		responseThree.setBackupFileKey("three");
		when(mockAsynchronousJobExecutor.executeSourceJob(any(AdminRequest.class), any())).thenReturn(responseOne, responseTwo, responseThree);
	}
	
	@Test
	public void testCreateBackupRequestsEmptyDestination() {
		this.batchSize = 3;
		TypeToMigrateMetadata typeToMigrate = new TypeToMigrateMetadata();
		typeToMigrate.setType(type);
		typeToMigrate.setSrcMinId(1L);
		typeToMigrate.setSrcMaxId(9L);
		// null max/min indicates there is no data in the destination.
		typeToMigrate.setDestMinId(null);
		typeToMigrate.setDestMaxId(null);
		typeToMigrate.setDestCount(null);
		List<BackupTypeRangeRequest> request = MissingFromDestinationIterator.createBackupRequests(typeToMigrate, aliasType, batchSize, destinationRowCountToIgnore);
		assertEquals(3, request.size());
		// one
		BackupTypeRangeRequest rangeRequest = request.get(0);
		assertEquals(new Long(batchSize), rangeRequest.getBatchSize());
		assertEquals(aliasType, rangeRequest.getAliasType());
		assertEquals(type, rangeRequest.getMigrationType());
		assertEquals(new Long(1), rangeRequest.getMinimumId());
		assertEquals(new Long(4), rangeRequest.getMaximumId());
		// two
		rangeRequest = request.get(1);
		assertEquals(new Long(batchSize), rangeRequest.getBatchSize());
		assertEquals(aliasType, rangeRequest.getAliasType());
		assertEquals(type, rangeRequest.getMigrationType());
		assertEquals(new Long(4), rangeRequest.getMinimumId());
		assertEquals(new Long(7), rangeRequest.getMaximumId());
		// three
		rangeRequest = request.get(2);
		assertEquals(new Long(batchSize), rangeRequest.getBatchSize());
		assertEquals(aliasType, rangeRequest.getAliasType());
		assertEquals(type, rangeRequest.getMigrationType());
		assertEquals(new Long(7), rangeRequest.getMinimumId());
		// since max is exclusive must add one to the last
		assertEquals(new Long(10), rangeRequest.getMaximumId());
	}
	
	@Test
	public void testCreateBackupRequestsWithDestination() {
		this.batchSize = 3;
		TypeToMigrateMetadata typeToMigrate = new TypeToMigrateMetadata();
		typeToMigrate.setType(type);
		typeToMigrate.setSrcMinId(1L);
		typeToMigrate.setSrcMaxId(9L);
		// null max/min indicates there is no data in the destination.
		typeToMigrate.setDestMinId(1L);
		typeToMigrate.setDestMaxId(2L);
		typeToMigrate.setDestCount(2L);
		List<BackupTypeRangeRequest> request = MissingFromDestinationIterator.createBackupRequests(typeToMigrate, aliasType, batchSize, destinationRowCountToIgnore);
		assertEquals(3, request.size());
		// one
		BackupTypeRangeRequest rangeRequest = request.get(0);
		assertEquals(new Long(3), rangeRequest.getMinimumId());
		assertEquals(new Long(6), rangeRequest.getMaximumId());
		// two
		rangeRequest = request.get(1);
		assertEquals(new Long(6), rangeRequest.getMinimumId());
		assertEquals(new Long(9), rangeRequest.getMaximumId());
		// three
		rangeRequest = request.get(2);
		assertEquals(new Long(9), rangeRequest.getMinimumId());
		assertEquals(new Long(10), rangeRequest.getMaximumId());
	}
	
	@Test
	public void testCreateBackupRequestsWithDestinationNoWork() {
		this.batchSize = 3;
		TypeToMigrateMetadata typeToMigrate = new TypeToMigrateMetadata();
		typeToMigrate.setType(type);
		typeToMigrate.setSrcMinId(1L);
		typeToMigrate.setSrcMaxId(9L);
		typeToMigrate.setDestMinId(1L);
		typeToMigrate.setDestMaxId(9L);
		typeToMigrate.setDestCount(8L);
		List<BackupTypeRangeRequest> request = MissingFromDestinationIterator.createBackupRequests(typeToMigrate, aliasType, batchSize, destinationRowCountToIgnore);
		assertEquals(0, request.size());
	}
	
	/**
	 * Even though the max ID of the destination is the same as the source,
	 * the number of rows in the destination is less than the count to ignore
	 * so a full backup/restore should occur.
	 */
	@Test
	public void testCreateBackupRequestsWithDestinationCountLow() {
		this.destinationRowCountToIgnore = 2;
		this.batchSize = 3;
		TypeToMigrateMetadata typeToMigrate = new TypeToMigrateMetadata();
		typeToMigrate.setType(type);
		typeToMigrate.setSrcMinId(1L);
		typeToMigrate.setSrcMaxId(9L);
		typeToMigrate.setDestMinId(1L);
		typeToMigrate.setDestMaxId(9L);
		typeToMigrate.setDestCount(destinationRowCountToIgnore-1);
		List<BackupTypeRangeRequest> request = MissingFromDestinationIterator.createBackupRequests(typeToMigrate, aliasType, batchSize, destinationRowCountToIgnore);
		assertEquals(3, request.size());
	}
	
	@Test
	public void testCreateBackupRequestsWithDestinationCountNull() {
		this.destinationRowCountToIgnore = 2;
		this.batchSize = 3;
		TypeToMigrateMetadata typeToMigrate = new TypeToMigrateMetadata();
		typeToMigrate.setType(type);
		typeToMigrate.setSrcMinId(1L);
		typeToMigrate.setSrcMaxId(9L);
		typeToMigrate.setDestMinId(1L);
		typeToMigrate.setDestMaxId(9L);
		// full table migration should occur if the destination count is null
		typeToMigrate.setDestCount(null);
		List<BackupTypeRangeRequest> request = MissingFromDestinationIterator.createBackupRequests(typeToMigrate, aliasType, batchSize, destinationRowCountToIgnore);
		assertEquals(3, request.size());
	}
	
	
	@Test
	public void testIterator() {
		TypeToMigrateMetadata typeToMigrate = new TypeToMigrateMetadata();
		typeToMigrate.setType(type);
		typeToMigrate.setSrcMinId(1L);
		typeToMigrate.setSrcMaxId(9L);
		typeToMigrate.setDestMinId(null);
		typeToMigrate.setDestMaxId(null);
		
		// call under test
		MissingFromDestinationIterator iterator = new MissingFromDestinationIterator(mockConfig, mockAsynchronousJobExecutor, typeToMigrate);
		// one
		assertTrue(iterator.hasNext());
		DestinationJob job = iterator.next();
		assertNotNull(job);
		assertTrue(job instanceof RestoreDestinationJob);
		RestoreDestinationJob restoreJob = (RestoreDestinationJob) job;
		assertEquals(type, job.getMigrationType());
		assertEquals(responseOne.getBackupFileKey(), restoreJob.getBackupFileKey());
		// two
		assertTrue(iterator.hasNext());
		job = iterator.next();
		assertNotNull(job);
		assertTrue(job instanceof RestoreDestinationJob);
		restoreJob = (RestoreDestinationJob) job;
		assertEquals(type, job.getMigrationType());
		assertEquals(responseTwo.getBackupFileKey(), restoreJob.getBackupFileKey());
		// three
		assertTrue(iterator.hasNext());
		job = iterator.next();
		assertNotNull(job);
		assertTrue(job instanceof RestoreDestinationJob);
		restoreJob = (RestoreDestinationJob) job;
		assertEquals(type, job.getMigrationType());
		assertEquals(responseThree.getBackupFileKey(), restoreJob.getBackupFileKey());
		// done
		assertFalse(iterator.hasNext());
		// three source jobs should be run
		verify(mockAsynchronousJobExecutor, times(3)).executeSourceJob(any(AdminRequest.class), any());
	}
}
