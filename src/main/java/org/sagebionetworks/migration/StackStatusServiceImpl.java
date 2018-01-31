package org.sagebionetworks.migration;

import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.repo.model.status.StackStatus;
import org.sagebionetworks.repo.model.status.StatusEnum;

import com.google.inject.Inject;

public class StackStatusServiceImpl implements StackStatusService {
	
	static final String READ_WRITE_MESSAGE = "Services are available for both read and write.";
	static final String READ_ONLY_MESSAGE = "Only read services are available during migration.";
	
	SynapseAdminClient destinationClient;
	SynapseAdminClient sourceClient;
	
	@Inject
	public StackStatusServiceImpl(SynapseClientFactory clientFactory) {
		// this module only uses the destination.
		destinationClient = clientFactory.getDestinationClient();
		sourceClient = clientFactory.getSourceClient();
	}

	@Override
	public void setDestinationReadOnly() {
		try {
			StackStatus status = destinationClient.getCurrentStackStatus();
			status.setCurrentMessage(READ_ONLY_MESSAGE);
			status.setStatus(StatusEnum.READ_ONLY);
			destinationClient.updateCurrentStackStatus(status);
		} catch (SynapseException e) {
			// throw a runtime to terminate migration
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setDestinationReadWrite() {
		try {
			StackStatus status = destinationClient.getCurrentStackStatus();
			status.setCurrentMessage(READ_WRITE_MESSAGE);
			status.setStatus(StatusEnum.READ_WRITE);
			destinationClient.updateCurrentStackStatus(status);
		} catch (SynapseException e) {
			// throw a runtime to terminate migration.
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean isSourceReadOnly() {
		try {
			StackStatus status = sourceClient.getCurrentStackStatus();
			return StatusEnum.READ_ONLY == status.getStatus();
		} catch (SynapseException e) {
			// throw a runtime to terminate migration.
			throw new RuntimeException(e);
		}
	}

}
