package org.sagebionetworks.migration.async;

import java.util.concurrent.Future;

import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.migration.AsyncMigrationException;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.AdminRequest;
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.repo.model.migration.AsyncMigrationRequest;

import com.google.inject.Inject;

/**
 * Simple implementation of an Asynchronous Job Executor.
 *
 */
public class AsynchronousJobExecutorImpl implements AsynchronousJobExecutor{

	SynapseAdminClient sourceClient;
	SynapseAdminClient destinationClient;
	FutureFactory futureFactory;
	long timeoutMS;


	/**
	 * @param sourceClient Client pointing to the source stack.
	 * @param destClient Client pointing to the destination stack.
	 * @param timeoutMS job timeout in MS.
	 */
	@Inject
	public AsynchronousJobExecutorImpl(SynapseClientFactory clientFactory, Configuration config, FutureFactory futureFactory) {
		super();
		this.sourceClient = clientFactory.getSourceClient();
		this.destinationClient = clientFactory.getDestinationClient();
		this.futureFactory = futureFactory;
		this.timeoutMS = config.getWorkerTimeoutMs();
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.migration.async.AsynchronousJobExecutor#executeSourceJob(org.sagebionetworks.repo.model.migration.AdminRequest, java.lang.Class)
	 */
	@Override
	public <I extends AdminRequest, O extends AdminResponse> O executeSourceJob(I request,
			Class<? extends O> reponseClass) {
		try {
			// start the job
			Future<O> future = startSourceJob(request, reponseClass);
			// wait for the job to finish.
			return future.get();
		}catch(Exception e) {
			throw new AsyncMigrationException(e);
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.migration.async.AsynchronousJobExecutor#executeDestinationJob(org.sagebionetworks.repo.model.migration.AdminRequest, java.lang.Class)
	 */
	@Override
	public <I extends AdminRequest, O extends AdminResponse> O executeDestinationJob(I request,
			Class<? extends O> reponseClass)  {
		try {
			// start the job
			Future<O> future = startDestionationJob(request, reponseClass);
			// wait for the job to finish.
			return future.get();
		}catch(Exception e) {
			throw new AsyncMigrationException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.migration.async.AsynchronousJobExecutor#executeSourceAndDestinationJob(org.sagebionetworks.repo.model.migration.AdminRequest, java.lang.Class)
	 */
	@Override
	public <I extends AdminRequest, O extends AdminResponse> ResultPair<O> executeSourceAndDestinationJob(I request,
			Class<? extends O> reponseClass) {
		try {
			// start the job on the source and destination.
			Future<O> sourceFuture = startSourceJob(request, reponseClass);
			Future<O> destinationFuture = startDestionationJob(request, reponseClass);
			// wait for both results.
			ResultPair<O> resultPair = new ResultPair<>();
			resultPair.setSourceResult(sourceFuture.get());
			resultPair.setDestinationResult(destinationFuture.get());
			return resultPair;
		}catch(Exception e) {
			throw new AsyncMigrationException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.migration.async.AsynchronousJobExecutor#startSourceJob(org.sagebionetworks.repo.model.migration.AdminRequest, java.lang.Class)
	 */
	@Override
	public <I extends AdminRequest, O extends AdminResponse> Future<O> startSourceJob(I request,
			Class<? extends O> reponseClass) {
		return startJob(JobTarget.SOURCE, request, reponseClass);
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.migration.async.AsynchronousJobExecutor#startDestionationJob(org.sagebionetworks.repo.model.migration.AdminRequest, java.lang.Class)
	 */
	@Override
	public <I extends AdminRequest, O extends AdminResponse> Future<O> startDestionationJob(I request,
			Class<? extends O> reponseClass) {
		return startJob(JobTarget.DESTINATION, request, reponseClass);
	}
	
	/**
	 * Start the given request on the provided client.  Return a future to be used to get the results when
	 * the job is complete.
	 * @param mockClient
	 * @param request
	 * @param reponseClass
	 * @return
	 */
	<I extends AdminRequest, O extends AdminResponse> Future<O> startJob(JobTarget jobTarget, I request,
			Class<? extends O> reponseClass) {
		try {
			// start the job
			AsyncMigrationRequest migRequest = new AsyncMigrationRequest();
			migRequest.setAdminRequest(request);
			SynapseAdminClient client = getClientForJobTarget(jobTarget);
			AsynchronousJobStatus jobStatus = client.startAdminAsynchronousJob(migRequest);
			// create a future to track the job.
			return futureFactory.createFuture(jobStatus, jobTarget, client, reponseClass);
		} catch (SynapseException e) {
			throw new AsyncMigrationException(e);
		}
	}

	/**
	 * Get the client to use with a given target.
	 * @param jobTarget
	 * @return
	 */
	SynapseAdminClient getClientForJobTarget(JobTarget jobTarget) {
		switch (jobTarget) {
		case SOURCE:
			return sourceClient;
		case DESTINATION:
			return destinationClient;
		default:
			throw new IllegalArgumentException("Unknown job target: " + jobTarget);
		}
	}
}
