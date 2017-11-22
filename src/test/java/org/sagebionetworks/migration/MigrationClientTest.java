package org.sagebionetworks.migration;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;

import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseServerException;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.repo.model.asynch.AsynchJobState;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.*;
import org.sagebionetworks.repo.model.status.StackStatus;
import org.sagebionetworks.repo.model.status.StatusEnum;
import org.sagebionetworks.simpleHttpClient.SimpleHttpClientConfig;
import org.sagebionetworks.migration.factory.SynapseClientFactory;

/**
 * Migration client test.
 * 
 * @author jmhill
 *
 */
public class MigrationClientTest {

	private SynapseAdminClientMockState mockDestination;
	private SynapseAdminClient destSynapse;
	
	private SynapseAdminClientMockState mockSource;
	private SynapseAdminClient sourceSynapse;
	
	private SynapseClientFactory mockClientFactory;
	private MigrationClient migrationClient;
	
	@Before
	public void before() throws Exception {
		// Create the two stubs
		mockDestination = new SynapseAdminClientMockState();
		mockDestination.endpoint = "destination";
		destSynapse = SynapseAdminClientMocker.createMock(mockDestination);
		
		mockSource = new SynapseAdminClientMockState();
		mockSource.endpoint = "source";
		sourceSynapse = SynapseAdminClientMocker.createMock(mockSource);
		
		mockClientFactory = Mockito.mock(SynapseClientFactory.class);
		when(mockClientFactory.getDestinationClient()).thenReturn(destSynapse);
		when(mockClientFactory.getSourceClient()).thenReturn(sourceSynapse);

		migrationClient = new MigrationClient(mockClientFactory, 1000);
	}
	
	// Used to fail after moving to SimpleHttpClient
	@Test
	public void testLogging() throws Exception {
		SimpleHttpClientConfig config = new SimpleHttpClientConfig();
		config.setConnectTimeoutMs(1000*60);
		config.setSocketTimeoutMs(1000*60*10);
		SynapseAdminClientImpl synapse = new SynapseAdminClientImpl(config);
	}

	@Test
	public void testSetDestinationStatus() throws Exception {
		// Set the status to down
		migrationClient.setDestinationStatus(StatusEnum.READ_ONLY, "Test message");
		// Only the destination should be changed
		StackStatus status = destSynapse.getCurrentStackStatus();
		StackStatus expected = new StackStatus();
		expected.setCurrentMessage("Test message");
		expected.setStatus(StatusEnum.READ_ONLY);
		assertEquals(expected, status);
		// The source should remain unmodified
		status = sourceSynapse.getCurrentStackStatus();
		expected = new StackStatus();
		expected.setCurrentMessage("Synapse is read for read/write");
		expected.setStatus(StatusEnum.READ_WRITE);
		assertEquals(expected, status);
	}
	
	@Test
	public void testGetCommonMigrationTypes() throws Exception {
		MigrationTypeNames expectedSrcTypeNames = new MigrationTypeNames();
		expectedSrcTypeNames.setList(Arrays.asList("PRINCIPAL", "GROUP_MEMBERS", "CREDENTIAL"));
		MigrationTypeNames expectedDestTypeNames = new MigrationTypeNames();
		expectedDestTypeNames.setList(Arrays.asList("PRINCIPAL", "CREDENTIAL", "PRINCIPAL_ALIAS"));
		List<MigrationType> expectedCommonTypes = Arrays.asList(MigrationType.PRINCIPAL, MigrationType.CREDENTIAL);

		SynapseAdminClient mockSrc = Mockito.mock(SynapseAdminClient.class);
		when(mockSrc.getMigrationTypeNames()).thenReturn(expectedSrcTypeNames);
		SynapseAdminClient mockDest = Mockito.mock(SynapseAdminClient.class);
		when(mockDest.getMigrationTypeNames()).thenReturn(expectedDestTypeNames);

		SynapseClientFactory mf = Mockito.mock(SynapseClientFactory.class);
		when(mf.getSourceClient()).thenReturn(mockSrc);
		when(mf.getDestinationClient()).thenReturn(mockDest);

		MigrationClient migClient = new MigrationClient(mf, 1000);

		List<MigrationType> commonTypes = migClient.getCommonMigrationTypes();
		assertEquals(expectedCommonTypes, commonTypes);
	}

	/**
	 * Test the full migration of data from the source to destination.
	 */
	@Test
	public void testMigrateTypes() throws Exception{
		// Setup the destination
		// The first element should get deleted and second should get updated.
		List<RowMetadata> list = createRowMetadataList(new Long[]{1L, 2L}, new String[]{"e1","e2"}, new Long[]{null, null});
		mockDestination.metadata.put(MigrationType.values()[0], list);
		
		// Setup a second type with no values
		list = createRowMetadataList(new Long[]{}, new String[]{}, new Long[]{});
		mockDestination.metadata.put(MigrationType.values()[1], list);

		// Setup a CHANGE to migrate (update)
		list = createRowMetadataList(new Long[]{10L}, new String[]{"ec"}, new Long[]{null});
		mockDestination.metadata.put(MigrationType.CHANGE, list);
		
		mockDestination.currentChangeNumberStack.push(11L);
		mockDestination.currentChangeNumberStack.push(0L);
		mockDestination.maxChangeNumber = 11L;
		
		// setup the source
		// The first element should get trigger an update and the second should trigger an add
		list = createRowMetadataList(new Long[]{2L, 3L}, new String[]{"e2changed","e3"}, new Long[]{null, 1l});
		mockSource.metadata.put(MigrationType.values()[0], list);
		
		// both values should get added
		list = createRowMetadataList(new Long[]{5L, 6L}, new String[]{"e5","e6"}, new Long[]{null, 6L});
		mockSource.metadata.put(MigrationType.values()[1], list);

		// Setup a CHANGE to migrate (update)
		list = createRowMetadataList(new Long[]{10L}, new String[]{"ed"}, new Long[]{null});
		mockSource.metadata.put(MigrationType.CHANGE, list);

		List<TypeToMigrateMetadata> typesToMigrateMetadata = createTypeToMigrateMetadataList(
				new MigrationType[]{MigrationType.values()[0], MigrationType.values()[1], MigrationType.CHANGE},
				new Long[]{2L, 5L, 10L}, new Long[]{3L, 6L, 10L}, new Long[]{2L, 2L, 1L},
				new Long[]{1L, 0L, 10L}, new Long[]{2L, 0L, 10L}, new Long[]{1L, 0L, 1L});
		
		// Migrate the data
		migrationClient.migrateTypes(typesToMigrateMetadata, 10L, 10L, 1000*60);
		
		// Now validate the results
		List<RowMetadata> expected0 = createRowMetadataList(new Long[]{2L, 3L}, new String[]{"e2changed","e3"}, new Long[]{null, 1l});
		List<RowMetadata> expected1 = createRowMetadataList(new Long[]{5L, 6L}, new String[]{"e5","e6"}, new Long[]{null, 6L});
		
		// check the state of the destination.
		assertEquals(expected0, mockDestination.metadata.get(MigrationType.values()[0]));
		assertEquals(expected1, mockDestination.metadata.get(MigrationType.values()[1]));
		
		// Check the state of the source
		assertEquals(expected0, mockSource.metadata.get(MigrationType.values()[0]));
		assertEquals(expected1, mockSource.metadata.get(MigrationType.values()[1]));
		
		// no messages should have been played on the destination.
		assertEquals(0, mockDestination.replayChangeNumbersHistory.size());
		
		// No messages should have been played on the source
		assertEquals(0, mockSource.replayChangeNumbersHistory.size());
	}
	
	/**
	 * Helper to build up lists of metadata
	 */
	public static List<RowMetadata> createRowMetadataList(Long[] ids, String[] etags, Long[] parentId){
		List<RowMetadata> list = new LinkedList<RowMetadata>();
		for (int i=0;  i<ids.length; i++) {
			if (ids[i] == null) {
				list.add(null);
			} else {
				RowMetadata row = new RowMetadata();
				row.setId(ids[i]);
				row.setEtag(etags[i]);
				row.setParentId(parentId[i]);
				list.add(row);
			}
		}
		return list;
	}

	public static List<TypeToMigrateMetadata> createTypeToMigrateMetadataList(MigrationType[] types, Long[] srcMins, Long[] srcMaxs, Long[] srcCounts, Long[] destMins, Long[] destMaxs, Long[] destCounts) {
		List<TypeToMigrateMetadata> l = new LinkedList<TypeToMigrateMetadata>();
		for (int i = 0;  i < types.length; i++) {
			if (types[i] == null) {
				l.add(null);
			} else {
				TypeToMigrateMetadata tm = new TypeToMigrateMetadata();
				tm.setType(types[i]);
				tm.setSrcMinId(srcMins[i]);
				tm.setSrcMaxId(srcMaxs[i]);
				tm.setSrcCount(srcCounts[i]);
				tm.setDestMinId(destMins[i]);
				tm.setDestMaxId(destMaxs[i]);
				tm.setDestCount(destCounts[i]);
				l.add(tm);
			}
		}
		return l;
	}

}
