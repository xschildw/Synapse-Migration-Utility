package org.sagebionetworks.migration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * The main entry point for the migration process
 *
 */
public class MigrationClientMain {
	
	static private Logger logger = LogManager.getLogger(MigrationClientMain.class);

	public static void main(String[] args) throws Exception {
		// Start IoC
		Injector injector = Guice.createInjector(new MigrationModule());
		MigrationClient client = injector.getInstance(MigrationClient.class);
		try {
			client.migrate();
		}catch(Throwable e) {
			logger.error("Migration failed: ",e);
			// -1 signals the failure to the caller.
			System.exit(-1);
		}
	}
	
}
