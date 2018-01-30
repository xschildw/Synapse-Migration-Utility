package org.sagebionetworks.migration.config;

import java.util.Properties;

/**
 * Simple wrapper for getting the static System properties.
 *
 */
public class SystemPropertiesProviderImpl implements SystemPropertiesProvider {

	@Override
	public Properties getSystemProperties() {
		return System.getProperties();
	}

	@Override
	public Properties createNewProperties() {
		return new Properties();
	}
	
}
