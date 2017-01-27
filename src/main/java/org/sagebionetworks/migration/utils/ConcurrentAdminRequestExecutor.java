package org.sagebionetworks.migration.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.migration.AsyncMigrationWorker;
import org.sagebionetworks.repo.model.migration.AdminRequest;
import org.sagebionetworks.repo.model.migration.AdminResponse;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

public class ConcurrentAdminRequestExecutor {

	static private Logger logger = LogManager.getLogger(ConcurrentAdminRequestExecutor.class);

	ExecutorService threadPool;

	public ConcurrentAdminRequestExecutor(ExecutorService threadPool) {
		this.threadPool = threadPool;
	}

	List<AdminResponse> executeRequests(List<AsyncMigrationWorker> workers) throws InterruptedException, ExecutionException {
		List<Future<AdminResponse>> futureResponses = new LinkedList<Future<AdminResponse>>();
		for (AsyncMigrationWorker w: workers) {
			futureResponses.add(this.threadPool.submit(w));
		}
		List<AdminResponse> responses = new LinkedList<AdminResponse>();
		for (Future<AdminResponse> fr: futureResponses) {
			try {
				responses.add(fr.get());
			} catch (ExecutionException e) {
				responses.add(null);
				logger.debug(e.getCause());
			} catch (InterruptedException e) {
				responses.add(null);
				logger.debug("Timeout excuting the asynchronous request.");
			}
		}
		return responses;
	}
}
