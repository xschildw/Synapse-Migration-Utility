package org.sagebionetworks.migration;

import java.io.IOException;

import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.migration.factory.SynapseClientFactoryImpl;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.sagebionetworks.migration.config.MigrationConfigurationImpl;
import org.sagebionetworks.migration.factory.SynapseClientFactory;

/**
 * The main entry point for the data migration process.
 * @author jmhill
 *
 */
public class MigrationClientMain {

	/**
	 * The main entry for for the V3 migration client.
	 * @param args
	 * @throws IOException 
	 * @throws SynapseException 
	 * @throws JSONObjectAdapterException 
	 */
	public static void main(String[] args) throws Exception {
		// First load the configuration
		MigrationConfigurationImpl configuration = new MigrationConfigurationImpl();
		loadCredentials(configuration, args);
		loadConfigUsingArgs(configuration, args);		
		// Create the client factory
		SynapseClientFactory factory = new SynapseClientFactoryImpl(configuration);
		MigrationClient client = new MigrationClient(factory);
		boolean failed = client.migrate(
				configuration.getMaxRetries(),
				configuration.getMaximumBackupBatchSize(),
				configuration.getMinimumRangeSize(),
				configuration.getWorkerTimeoutMs());
		if (failed) {
			System.exit(-1);
		} else {
			System.exit(0);
		}
	}
	
	/**
	 * Load the configuration using the passed args.
	 * @param configuration 
	 * @param args
	 * @throws IOException
	 */
	public static void loadConfigUsingArgs(MigrationConfigurationImpl configuration, String[] args) throws IOException {
		if (args != null && args.length == 2) {
			// Load and validate from file
			String path = args[1];
			configuration.loadConfigurationFile(path);
		}
		// Validate System properties
		configuration.validateConfigurationProperties();
	}
	
	public static void loadCredentials(MigrationConfigurationImpl configuration, String[] args) throws IOException {
		if (args != null && args.length >= 1) {
			String path = args[0];
			configuration.loadApiKey(path);
		} else {
			throw new IllegalArgumentException("Path to API Key file must be specified as first argument.");
		}
	}
}
