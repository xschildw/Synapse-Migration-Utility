package org.sagebionetworks.migration.factory;

import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;

/**
 * Factory for creating SynapseAdministration clients.
 * 
 * @author jmhill
 *
 */
public interface SynapseClientFactory {

	public SynapseAdminClient getSourceClient();
	public SynapseAdminClient getDestinationClient();
}
