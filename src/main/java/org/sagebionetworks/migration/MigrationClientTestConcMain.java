package org.sagebionetworks.migration;

import org.sagebionetworks.migration.config.MigrationConfigurationImpl;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.migration.factory.SynapseClientFactoryImpl;
import org.sagebionetworks.repo.model.migration.MigrationType;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class MigrationClientTestConcMain {
	public static void main(String[] args) throws Exception {
		MigrationConfigurationImpl configuration = new MigrationConfigurationImpl();
		loadCredentials(configuration, args);
		loadConfigUsingArgs(configuration, args);
		SynapseClientFactory factory = new SynapseClientFactoryImpl(configuration);
		MigrationClient client = new MigrationClient(factory);
		try {
			List<String> l = client.doConcurrentChecksumForType(MigrationType.V2_WIKI_PAGE);
			System.out.println(l.get(0) + " / " + l.get(1));
		} catch (ExecutionException e) {
			System.out.println("Error!\n\n" + e.getMessage());
		}

	}

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
