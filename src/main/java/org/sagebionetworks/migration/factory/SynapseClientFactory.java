package org.sagebionetworks.migration.factory;

import org.sagebionetworks.client.SynapseAdminClient;

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
