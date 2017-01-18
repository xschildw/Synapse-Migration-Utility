package org.sagebionetworks.migration.factory;

import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.simpleHttpClient.SimpleHttpClientConfig;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.config.SynapseConnectionInfo;

/**
 * Simple implementation of the client factory
 * 
 * @author jmhill
 *
 */
public class SynapseClientFactoryImpl implements SynapseClientFactory {

	
	Configuration config;
	
	/**
	 * New factory with the required connection information.
	 * @param config
	 */
	public SynapseClientFactoryImpl(Configuration config) {
		super();
		this.config = config;
	}

	@Override
	public SynapseAdminClientImpl createNewSourceClient() throws SynapseException {
		return createNewConnection(this.config.getSourceConnectionInfo());
	}

	@Override
	public SynapseAdminClientImpl createNewDestinationClient() throws SynapseException {
		return createNewConnection(this.config.getDestinationConnectionInfo());
	}
	
	/**
	 * Create a new Synapse connection using the provided information.
	 * @param info
	 * @return
	 * @throws SynapseException 
	 */
	private static SynapseAdminClientImpl createNewConnection(SynapseConnectionInfo info) throws SynapseException{
		SimpleHttpClientConfig config = new SimpleHttpClientConfig();
		config.setConnectTimeoutMs(1000*60);
		config.setSocketTimeoutMs(1000*60*10);
		SynapseAdminClientImpl synapse = new SynapseAdminClientImpl(config);
		synapse.setAuthEndpoint(info.getAuthenticationEndPoint());
		synapse.setRepositoryEndpoint(info.getRepositoryEndPoint());
		synapse.setUsername(info.getUserName());
		synapse.setApiKey(info.getApiKey());
		return synapse;
	}

}
