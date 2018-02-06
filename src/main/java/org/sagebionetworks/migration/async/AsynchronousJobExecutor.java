package org.sagebionetworks.migration.async;

import java.util.concurrent.Future;

import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.repo.model.migration.AdminRequest;
import org.sagebionetworks.repo.model.migration.AdminResponse;

/**
 * Abstraction for a Asynchronous Job Executor.
 */
public interface AsynchronousJobExecutor {

	/**
	 * Execute an Asynchronous job using the source client.
	 * 
	 * @param request
	 *            The AdminRequest defines the type of request to execute.
	 * @param reponseClass
	 *            Defines the response class.
	 * @return
	 */
	public <I extends AdminRequest, O extends AdminResponse> O executeSourceJob(I request,
			Class<? extends O> reponseClass);

	/**
	 * Execute an Asynchronous job using the source client.
	 * 
	 * @param request
	 *            The AdminRequest defines the type of request to execute.
	 * @param reponseClass
	 * @return
	 */
	public <I extends AdminRequest, O extends AdminResponse> O executeDestinationJob(I request,
			Class<? extends O> reponseClass);

	/**
	 * Run the given request on both the source and destination concurrently.
	 * 
	 * @param request
	 * @param reponseClass
	 * @return
	 */
	public <I extends AdminRequest, O extends AdminResponse> ResultPair<O> executeSourceAndDestinationJob(I request,
			Class<? extends O> reponseClass);
	
	/**
	 * Start the given request on the source and immediately return a future to be used to get the results when ready.
	 * @param request
	 * @param reponseClass
	 * @return
	 */
	public <I extends AdminRequest, O extends AdminResponse> Future<O> startSourceJob(I request,
			Class<? extends O> reponseClass);
	
	/**
	 * Start the given request on the destination and immediately return a future to be used to get the results when ready.
	 * @param request
	 * @param reponseClass
	 * @return
	 */
	public <I extends AdminRequest, O extends AdminResponse> Future<O> startDestionationJob(I request,
			Class<? extends O> reponseClass);
}
