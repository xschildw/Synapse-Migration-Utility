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

	/**
	 * The minimum size for a DeltaRange (before deltas are calculated serially vs checksum
	 *
	 * @return
	 */
	public int getMinimumDeltaRangeSize();

	public long getWorkerTimeoutMs();

	/**
	 * Maximum number of migration retries
	 */
	public int getMaxRetries();
	
	
	/**
	 * A full table migration will occur if the destination has less than this threshold percentage of rows.
	 * 
	 * @return percentage (float between zero and one)
	 */
	public float getFullTableMigrationThresholdPercentage();
	
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


}
