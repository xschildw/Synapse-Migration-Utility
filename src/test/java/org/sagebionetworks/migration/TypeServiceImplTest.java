package org.sagebionetworks.migration;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeNames;

import com.amazonaws.services.dynamodbv2.xspec.M;
import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class TypeServiceImplTest {

	@Mock
	SynapseClientFactory mockClientFactory;
	@Mock
	SynapseAdminClient mockSourceClient;
	@Mock
	SynapseAdminClient mockDestinationClient;

	List<String> sourceNames;
	List<String> sourcePrimaryNames;
	List<String> destinationNames;
	List<String> destinationPrimaryNames;

	TypeServiceImpl typeService;

	@Before
	public void before() throws SynapseException {
		when(mockClientFactory.getSourceClient()).thenReturn(mockSourceClient);
		when(mockClientFactory.getDestinationClient()).thenReturn(mockDestinationClient);
		typeService = new TypeServiceImpl(mockClientFactory);

		sourceNames = Lists.newArrayList(
				MigrationType.NODE.name(),
				MigrationType.NODE_REVISION.name(),
				MigrationType.ACTIVITY.name());
		destinationNames = Lists.newArrayList(
				MigrationType.ACL.name(),
				MigrationType.NODE.name(),
				MigrationType.NODE_REVISION.name(),
				MigrationType.ACTIVITY.name());
		
		sourcePrimaryNames = Lists.newArrayList(
				MigrationType.NODE.name(),
				MigrationType.ACTIVITY.name());
		destinationPrimaryNames = Lists.newArrayList(
				MigrationType.ACL.name(),
				MigrationType.NODE.name(),
				MigrationType.ACTIVITY.name());
		// mock source
		MigrationTypeNames typeNames = new MigrationTypeNames();
		typeNames.setList(sourceNames);
		when(mockSourceClient.getMigrationTypeNames()).thenReturn(typeNames);
		typeNames = new MigrationTypeNames();
		typeNames.setList(sourcePrimaryNames);
		when(mockSourceClient.getPrimaryTypeNames()).thenReturn(typeNames);
		// mock destination
		typeNames = new MigrationTypeNames();
		typeNames.setList(destinationNames);
		when(mockDestinationClient.getMigrationTypeNames()).thenReturn(typeNames);
		typeNames = new MigrationTypeNames();
		typeNames.setList(destinationPrimaryNames);
		when(mockDestinationClient.getPrimaryTypeNames()).thenReturn(typeNames);

	}

	@Test
	public void testGetNameIntersection() {
		List<String> one = Lists.newArrayList("1", "2", "3", "4");
		List<String> two = Lists.newArrayList("0", "1", "3", "5");
		List<String> expectedIntersection = Lists.newArrayList("1", "3");
		// call under test
		List<String> resutls = typeService.getNameIntersection(one, two);
		assertEquals(expectedIntersection, resutls);
	}

	@Test
	public void testGetMigrationTypeIntersection() {
		List<MigrationType> expectedIntersection = Lists.newArrayList(
				MigrationType.NODE,
				MigrationType.NODE_REVISION,
				MigrationType.ACTIVITY
		);
		// call under test
		List<MigrationType> typeIntersection = typeService.getMigrationTypeIntersection(sourceNames, destinationNames);
		assertEquals(expectedIntersection, typeIntersection);
	}
	
	@Test
	public void testGetAllCommonMigrationTypes() {
		List<MigrationType> expected = Lists.newArrayList(
				MigrationType.NODE,
				MigrationType.NODE_REVISION,
				MigrationType.ACTIVITY
		);
		// call under test
		List<MigrationType> results = typeService.getAllCommonMigrationTypes();
		assertEquals(expected, results);
	}
	
	@Test
	public void testGetCommonPrimaryMigrationTypes() {
		List<MigrationType> expected = Lists.newArrayList(
				MigrationType.NODE,
				MigrationType.ACTIVITY
		);
		// call under test
		List<MigrationType> results = typeService.getCommonPrimaryMigrationTypes();
		assertEquals(expected, results);
	}

}
