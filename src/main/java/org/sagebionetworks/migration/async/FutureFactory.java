package org.sagebionetworks.migration.async;

import java.util.concurrent.Future;

import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.repo.model.migration.MigrationType;

/**
 * 
 * Also known as the present.
 *
 */
public interface FutureFactory {

	/**
	 * Create a future to track a job.
	 * 
	 * @param jobStatus Job status to track.
	 * @param jobTarget Target where this job is running.
	 * @param client The client used to start and track the job.
	 * @param reponseClass response type.
	 * @return
	 */
	public <O extends AdminResponse> Future<O> createFuture(AsynchronousJobStatus jobStatus, JobTarget jobTarget,
			SynapseAdminClient client, Class<? extends O> reponseClass);

}
