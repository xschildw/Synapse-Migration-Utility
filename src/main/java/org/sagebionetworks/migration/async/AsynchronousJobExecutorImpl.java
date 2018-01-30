package org.sagebionetworks.migration.async;

import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.repo.model.migration.AdminRequest;
import org.sagebionetworks.repo.model.migration.AdminResponse;

import com.google.inject.Inject;

/**
 * Simple implementation of an Asynchronous Job Executor.
 * 
 * @author John
 *
 */
public class AsynchronousJobExecutorImpl implements AsynchronousJobExecutor{

	SynapseClientFactory clientFactory;
	long timeoutMS;


	/**
	 * @param sourceClient Client pointing to the source stack.
	 * @param destClient Client pointing to the destination stack.
	 * @param timeoutMS job timeout in MS.
	 */
	@Inject
	public AsynchronousJobExecutorImpl(SynapseClientFactory clientFactory, Configuration config) {
		super();
		this.clientFactory = clientFactory;
		this.timeoutMS = config.getWorkerTimeoutMs();
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.migration.async.AsynchronousJobExecutor#executeSourceJob(org.sagebionetworks.repo.model.migration.AdminRequest, java.lang.Class)
	 */
	@Override
	public <I extends AdminRequest, O extends AdminResponse> O executeSourceJob(I request,
			Class<? extends O> reponseClass) throws AsyncMigrationException, InterruptedException {
		AsyncMigrationRequestExecutor<I, O> 
		backupExecutor = new AsyncMigrationRequestExecutor<I, O>(
				clientFactory.getSourceClient(), request, this.timeoutMS);
		return backupExecutor.execute();
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.migration.async.AsynchronousJobExecutor#executeDestinationJob(org.sagebionetworks.repo.model.migration.AdminRequest, java.lang.Class)
	 */
	@Override
	public <I extends AdminRequest, O extends AdminResponse> O executeDestinationJob(I request,
			Class<? extends O> reponseClass) throws AsyncMigrationException, InterruptedException {
		AsyncMigrationRequestExecutor<I, O> 
		backupExecutor = new AsyncMigrationRequestExecutor<I, O>(
				clientFactory.getDestinationClient(), request, this.timeoutMS);
		return backupExecutor.execute();
	}
}
