package org.sagebionetworks.migration.config;

import java.util.Properties;

/**
 * Abstraction for getting system properties.
 *
 */
public interface SystemPropertiesProvider {

	/**
	 * Get the system properties.
	 * @return
	 */
	public Properties getSystemProperties();
	
	/**
	 * Create new Properties object.
	 * @return
	 */
	public Properties createNewProperties();
}
