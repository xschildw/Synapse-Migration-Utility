package org.sagebionetworks.migration;

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

}
