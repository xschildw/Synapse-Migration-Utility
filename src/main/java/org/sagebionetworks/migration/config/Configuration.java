package org.sagebionetworks.migration.config;

import org.sagebionetworks.repo.model.daemon.BackupAliasType;

/**
 * Provides configuration information
 * 
 * @author John
 * 
 */
public interface Configuration {

	/**
	 * Get the source connection information.
	 * 
	 * @return
	 */
	public SynapseConnectionInfo getSourceConnectionInfo();

	/**
	 * Get the destination connection information.
	 * 
	 * @return
	 */
	public SynapseConnectionInfo getDestinationConnectionInfo();

	public int getMaximumNumberThreads();

	/**
	 * The Maximum batch size.
	 * 
	 * @return
	 */
	public int getMaximumBackupBatchSize();


	public long getWorkerTimeoutMs();

	/**
	 * Maximum number of migration retries
	 */
	public int getMaxRetries();
	
	
	/**
	 * The type of alias that should be used when writing and reading 
	 * backup files.
	 * @return
	 */
	public BackupAliasType getBackupAliasType();

	/**
	 * Should full table check-sums be run?
	 * @return
	 */
	public boolean includeFullTableChecksums();

	/**
	 * Log the configuration.
	 */
	public void logConfiguration();
	
	/**
	 * Get the number of MS that will be used as a delay before starting migration.
	 * @return
	 */
	public long getDelayBeforeMigrationStartMS();
	
	/**
	 * Should the destination stack remain in read-only mode after successful migration?
	 * @return By default returns false.  Override 
	 */
	public boolean remainInReadOnlyAfterMigration();

}
