package org.sagebionetworks.migration;

import java.util.Timer;
import java.util.TimerTask;

import org.sagebionetworks.migration.async.AsynchronousJobExecutor;
import org.sagebionetworks.migration.async.AsynchronousJobExecutorImpl;
import org.sagebionetworks.migration.async.BackupJobExecutor;
import org.sagebionetworks.migration.async.BackupJobExecutorImpl;
import org.sagebionetworks.migration.async.DestinationJobBuilder;
import org.sagebionetworks.migration.async.DestinationJobBuilderImpl;
import org.sagebionetworks.migration.async.DestinationJobExecutor;
import org.sagebionetworks.migration.async.DestinationJobExecutorImpl;
import org.sagebionetworks.migration.async.MigrationDriver;
import org.sagebionetworks.migration.async.MigrationDriverImpl;
import org.sagebionetworks.migration.async.MissingFromDestinationBuilder;
import org.sagebionetworks.migration.async.MissingFromDestinationBuilderImpl;
import org.sagebionetworks.migration.async.RestoreJobQueue;
import org.sagebionetworks.migration.async.RestoreJobQueueImpl;
import org.sagebionetworks.migration.async.checksum.ChecksumDeltaBuilder;
import org.sagebionetworks.migration.async.checksum.ChecksumDeltaBuilderImpl;
import org.sagebionetworks.migration.async.checksum.RangeCheksumBuilder;
import org.sagebionetworks.migration.async.checksum.RangeCheksumBuilderImpl;
import org.sagebionetworks.migration.async.checksum.TypeChecksumBuilder;
import org.sagebionetworks.migration.async.checksum.TypeChecksumBuilderImpl;
import org.sagebionetworks.migration.async.FutureFactory;
import org.sagebionetworks.migration.async.FutureFactoryImpl;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.config.FileProvider;
import org.sagebionetworks.migration.config.FileProviderImp;
import org.sagebionetworks.migration.config.MigrationConfigurationImpl;
import org.sagebionetworks.migration.config.SystemPropertiesProvider;
import org.sagebionetworks.migration.config.SystemPropertiesProviderImpl;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.migration.factory.SynapseClientFactoryImpl;
import org.sagebionetworks.util.Clock;
import org.sagebionetworks.util.DefaultClock;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class MigrationModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(LoggerFactory.class).to(LoggerFactoryImpl.class);
		bind(StackStatusService.class).to(StackStatusServiceImpl.class);
		bind(FileProvider.class).to(FileProviderImp.class);
		bind(SystemPropertiesProvider.class).to(SystemPropertiesProviderImpl.class);
		bind(Configuration.class).to(MigrationConfigurationImpl.class);
		bind(SynapseClientFactory.class).to(SynapseClientFactoryImpl.class);
		bind(AsynchronousJobExecutor.class).to(AsynchronousJobExecutorImpl.class);
		bind(MigrationClient.class).to(MigrationClientImpl.class);
		bind(FullMigration.class).to(FullMigrationImpl.class);
		bind(FutureFactory.class).to(FutureFactoryImpl.class);
		bind(Clock.class).to(DefaultClock.class);
		bind(Reporter.class).to(ReporterImpl.class);
		bind(TypeService.class).to(TypeServiceImpl.class);
		bind(MigrationDriver.class).to(MigrationDriverImpl.class);
		bind(DestinationJobBuilder.class).to(DestinationJobBuilderImpl.class);
		bind(DestinationJobExecutor.class).to(DestinationJobExecutorImpl.class);
		bind(MissingFromDestinationBuilder.class).to(MissingFromDestinationBuilderImpl.class);
		bind(BackupJobExecutor.class).to(BackupJobExecutorImpl.class);
		bind(ChecksumDeltaBuilder.class).to(ChecksumDeltaBuilderImpl.class);
		bind(TypeChecksumBuilder.class).to(TypeChecksumBuilderImpl.class);
		bind(RangeCheksumBuilder.class).to(RangeCheksumBuilderImpl.class);
	}
	
	@Provides
	public AWSSecretsManager provideAWSSecretsManager() {
	    AWSSecretsManagerClientBuilder builder = AWSSecretsManagerClientBuilder.standard();
		builder.withCredentials(new DefaultAWSCredentialsProviderChain());
		builder.withRegion(Regions.US_EAST_1);
	    return builder.build();
	}
	
	/**
	 * Setup the RestoreJobQueue with a timer thread.
	 * 
	 * @param jobExecutor
	 * @param loggerFactory
	 * @return
	 */
	@Provides
	public RestoreJobQueue provideRestorJobQueue(DestinationJobExecutor jobExecutor, LoggerFactory loggerFactory) {
		// setup the queue to run on a timer.
		RestoreJobQueueImpl queue = new RestoreJobQueueImpl(jobExecutor, loggerFactory);
		long delayMS = 100;
		long periodMS = 1000;
		MigrationModule.startDaemonTimer(delayMS, periodMS, queue);
		return queue;
	}
	
	/**
	 * Start a daemon timer to fire the passed runnable.
	 * 
	 * @param delayMS Delay to start the timer in MS.
	 * @param periodMs The period the timer will fire in MS.
	 * @param runner The Runnable.run() method will be called each time the timer is fired.
	 */
	public static Timer startDaemonTimer(long delayMS, long periodMS, Runnable runner) {
		// Setup the timer
		boolean isDaemon = true;
		Timer timer = new Timer(isDaemon);
		timer.schedule(new TimerTask() {

			@Override
			public void run() {
				runner.run();
			}
		}, delayMS, periodMS);
		return timer;
	}

}
