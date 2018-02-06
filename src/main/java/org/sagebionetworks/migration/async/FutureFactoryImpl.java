package org.sagebionetworks.migration.async;

import java.util.concurrent.Future;

import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.migration.Reporter;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.repo.model.asynch.AsynchronousJobStatus;
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.util.Clock;

import com.google.inject.Inject;

public class FutureFactoryImpl implements FutureFactory {
	
	Reporter reporter;
	Clock clock;
	Configuration configuration;

	@Inject
	public FutureFactoryImpl(Reporter reporter, Clock clock, Configuration configuration) {
		super();
		this.reporter = reporter;
		this.clock = clock;
		this.configuration = configuration;
	}

	@Override
	public <O extends AdminResponse> Future<O> createFuture(AsynchronousJobStatus jobStatus, JobTarget jobTarget,
			SynapseAdminClient client, Class<? extends O> reponseClass) {
		// create a new future for each call.
		return new AsynchronousJobFuture<O>(reporter, clock, jobStatus, jobTarget, client, configuration.getWorkerTimeoutMs());
	}

}
