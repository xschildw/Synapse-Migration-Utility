package org.sagebionetworks.migration.async;

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
	 * @param request The AdminRequest defines the type of request to execute.
	 * @param reponseClass Defines the response class.
	 * @return
	 * @throws InterruptedException 
	 * @throws AsyncMigrationException 
	 */
	public <I extends AdminRequest, O extends AdminResponse> O executeSourceJob(I request, Class<? extends O> reponseClass) throws AsyncMigrationException, InterruptedException;
	
	/**
	 * Execute an Asynchronous job using the source client.
	 * 
	 * @param request The AdminRequest defines the type of request to execute.
	 * @param reponseClass
	 * @return
	 * @throws InterruptedException 
	 * @throws AsyncMigrationException 
	 */
	public <I extends AdminRequest, O extends AdminResponse> O executeDestinationJob(I request, Class<? extends O> reponseClass) throws AsyncMigrationException, InterruptedException;

}
