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
	 * The Maximum backup batch size.
	 * 
	 * @return
	 */
	public int getMaximumBackupBatchSize();

	/**
	 * The minimum range size before searching for diffs serially
	 *
	 * @return
	 */
	public int getMinimumRangeSize();

	public long getWorkerTimeoutMs();

	/**
	 * If a DaemonFailedException is thrown by the worker, then the batch will be
	 * divide the batch into sub-batches using this number as the denominator.
	 * An attempt will then be made to retry each sub-batch. If this is set to
	 * less than 2, then no re-try will be attempted.
	 * 
	 * @return
	 */
	public int getRetryDenominator();
	
	/**
	 * Maximum number of migration retries
	 */
	public int getMaxRetries();
	
	/**
	 * Defer exceptions
	 */
	public boolean getDeferExceptions();

}
