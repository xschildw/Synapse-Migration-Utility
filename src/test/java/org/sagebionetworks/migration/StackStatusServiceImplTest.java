package org.sagebionetworks.migration;

import static org.mockito.Mockito.verify;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.client.exceptions.SynapseServerException;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.repo.model.status.StackStatus;
import org.sagebionetworks.repo.model.status.StatusEnum;

@RunWith(MockitoJUnitRunner.class)
public class StackStatusServiceImplTest {
	
	@Mock
	SynapseClientFactory mockClientFactory;
	@Mock
	SynapseAdminClient mockDestinationClient;
	@Mock
	SynapseAdminClient mockSourceClient;

	StackStatusServiceImpl service;
	
	@Before
	public void before() throws SynapseException {
		when(mockClientFactory.getDestinationClient()).thenReturn(mockDestinationClient);
		when(mockClientFactory.getSourceClient()).thenReturn(mockSourceClient);
		StackStatus startStatus = new StackStatus();
		startStatus.setCurrentMessage("starting message");
		startStatus.setStatus(StatusEnum.DOWN);
		when(mockDestinationClient.getCurrentStackStatus()).thenReturn(startStatus);
		service = new StackStatusServiceImpl(mockClientFactory);
	}
	
	@Test
	public void testSetDestinationReadOnly() throws SynapseException {
		// call under test
		service.setDestinationReadOnly();
		StackStatus expectedStatus = new StackStatus();
		expectedStatus.setCurrentMessage(StackStatusServiceImpl.READ_ONLY_MESSAGE);
		expectedStatus.setStatus(StatusEnum.READ_ONLY);
		verify(mockDestinationClient).updateCurrentStackStatus(expectedStatus);
		verifyZeroInteractions(mockSourceClient);
	}
	
	@Test (expected=RuntimeException.class)
	public void testSetDestinationReadOnlyException() throws SynapseException {
		SynapseServerException failure = new SynapseServerException(500);
		when(mockDestinationClient.getCurrentStackStatus()).thenThrow(failure);
		// call under test
		service.setDestinationReadOnly();
	}
	
	@Test
	public void testSetDestinationReadWrite() throws SynapseException {
		// call under test
		service.setDestinationReadWrite();
		StackStatus expectedStatus = new StackStatus();
		expectedStatus.setCurrentMessage(StackStatusServiceImpl.READ_WRITE_MESSAGE);
		expectedStatus.setStatus(StatusEnum.READ_WRITE);
		verify(mockDestinationClient).updateCurrentStackStatus(expectedStatus);
		verifyZeroInteractions(mockSourceClient);
	}
	
	@Test (expected=RuntimeException.class)
	public void testSetDestinationReadWriteException() throws SynapseException {
		SynapseServerException failure = new SynapseServerException(500);
		when(mockDestinationClient.getCurrentStackStatus()).thenThrow(failure);
		// call under test
		service.setDestinationReadWrite();
	}
	
	@Test
	public void testIsSourceReadOnly() throws SynapseException {
		StackStatus status = new StackStatus();
		status.setStatus(StatusEnum.READ_ONLY);
		when(mockSourceClient.getCurrentStackStatus()).thenReturn(status);
		// call under test
		boolean isReadOnly = service.isSourceReadOnly();
		assertTrue(isReadOnly);
	}
	
	@Test
	public void testIsSourceReadOnlyDown() throws SynapseException {
		StackStatus status = new StackStatus();
		status.setStatus(StatusEnum.DOWN);
		when(mockSourceClient.getCurrentStackStatus()).thenReturn(status);
		// call under test
		boolean isReadOnly = service.isSourceReadOnly();
		assertFalse(isReadOnly);
	}
	
	@Test
	public void testIsSourceReadOnlyReadWrite() throws SynapseException {
		StackStatus status = new StackStatus();
		status.setStatus(StatusEnum.READ_WRITE);
		when(mockSourceClient.getCurrentStackStatus()).thenReturn(status);
		// call under test
		boolean isReadOnly = service.isSourceReadOnly();
		assertFalse(isReadOnly);
	}
}
