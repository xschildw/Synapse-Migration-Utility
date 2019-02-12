package org.sagebionetworks.migration;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.*;

import java.util.Timer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.inject.Guice;
import com.google.inject.Injector;

@RunWith(MockitoJUnitRunner.class)
public class MigrationModuleTest {
	
	@Mock
	Runnable mockRunner;
	
	@Test
	public void testInjector() {
		// Simple test to determine if the Guice can build all of the dependencies.
		Injector injector = Guice.createInjector(new MigrationModule());
		assertNotNull(injector);
	}
	
	@Test
	public void testStartDaemonTimerPeriod() throws InterruptedException {
		long delayMS = 10;
		long periodMS = 100;
		Timer timer = MigrationModule.startDaemonTimer(delayMS, periodMS, mockRunner);
		assertNotNull(timer);
		// Wait for the timer to fire
		Thread.sleep(1000L);
		verify(mockRunner, atLeast(8)).run();
	}
	
	@Test
	public void testStartDaemonTimerDelay() throws InterruptedException {
		// the delay is longer than the test wait.
		long delayMS = 2000;
		long periodMS = 100;
		Timer timer = MigrationModule.startDaemonTimer(delayMS, periodMS, mockRunner);
		assertNotNull(timer);
		// Wait for the timer to fire
		Thread.sleep(1000L);
		verify(mockRunner, never()).run();
	}
	

}
