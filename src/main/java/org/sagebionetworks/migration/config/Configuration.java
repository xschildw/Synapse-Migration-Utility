package org.sagebionetworks.migration.config;

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
	
}
