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

	/**
	 * Create a new client connected to the source stack.
	 * @return
	 * @throws SynapseException
	 */
	public SynapseAdminClient createNewSourceClient();
	
	/**
	 * Create a new client connected to the destination stack
	 * @return
	 * @throws SynapseException
	 */
	public SynapseAdminClient createNewDestinationClient();

	public SynapseAdminClient getSourceClient();
	public SynapseAdminClient getDestinationClient();
}
