package org.sagebionetworks.migration.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.repo.model.migration.AdminRequest;
import org.sagebionetworks.tool.progress.BasicProgress;
import org.sagebionetworks.util.Clock;
import org.sagebionetworks.util.DefaultClock;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

public class ConcurrentMigrationWorker implements Callable<ConcurrentExecutionResult> {

	static private Logger logger = LogManager.getLogger(ConcurrentMigrationWorker.class);

	public ConcurrentMigrationWorker(Executor threadPool, SynapseClientFactory factory, AdminRequest srcReq, AdminRequest destReq, long timeOutMS, BasicProgress progress) {

	}

	@Override
	public ConcurrentExecutionResult call() {
		ConcurrentExecutionResult result = new ConcurrentExecutionResult();
		return result;
	}
}
