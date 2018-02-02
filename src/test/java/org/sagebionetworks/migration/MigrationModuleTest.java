package org.sagebionetworks.migration;

import static org.junit.Assert.assertNotNull;

import org.junit.Ignore;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class MigrationModuleTest {
	
	@Test
	public void testInjector() {
		// Simple test to determine if the Guice can build all of the dependencies.
		Injector injector = Guice.createInjector(new MigrationModule());
		assertNotNull(injector);
	}

}
