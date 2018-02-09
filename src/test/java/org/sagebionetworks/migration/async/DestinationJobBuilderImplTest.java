package org.sagebionetworks.migration.async;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.repo.model.migration.MigrationType;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class DestinationJobBuilderImplTest {
	
	@Mock
	MissingFromDestinationBuilder mockMissingFromDestinationBuilder;
	@Mock
	ChecksumDeltaBuilder mockChecksumChangeBuilder;
	
	DestinationJobBuilderImpl builder;
	
	List<TypeToMigrateMetadata> primaryTypes;
	
	@Before
	public void before() {
		primaryTypes = new LinkedList<>();
		List<DestinationJob> one = Lists.newArrayList(new RestoreDestinationJob(MigrationType.NODE, "one"));
		List<DestinationJob> two = Lists.newArrayList(new RestoreDestinationJob(MigrationType.NODE, "two"));
		when(mockMissingFromDestinationBuilder.buildDestinationJobs(primaryTypes)).thenReturn(one.iterator());
		when(mockChecksumChangeBuilder.buildChecksumJobs(primaryTypes)).thenReturn(two.iterator());
		
		builder = new DestinationJobBuilderImpl(mockMissingFromDestinationBuilder, mockChecksumChangeBuilder);
	}
	
	@Test
	public void testBuildDestinationJobs() {
		// call under test
		Iterator<DestinationJob> iterator = builder.buildDestinationJobs(primaryTypes);
		assertNotNull(iterator);
		verify(mockMissingFromDestinationBuilder).buildDestinationJobs(primaryTypes);
		verify(mockChecksumChangeBuilder).buildChecksumJobs(primaryTypes);
	}

}
