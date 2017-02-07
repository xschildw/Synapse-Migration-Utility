package org.sagebionetworks.migration.async;

import org.sagebionetworks.repo.model.migration.AdminResponse;

/**
 * A container for a pair of AdminResponse returned by ConcurrentAdminRequestExecutor.call()
 */
public class ConcurrentExecutionResult {

	private AdminResponse srcResponse;
	private AdminResponse destResponse;

	public void setSourceResponse(AdminResponse resp) {
		this.srcResponse = resp;
	}

	public AdminResponse getSourceResponse() {
		return this.srcResponse;
	}

	public void setDestinationResponse(AdminResponse resp) {
		this.destResponse = resp;
	}

	public AdminResponse getDestinationResponse() {
		return this.destResponse;
	}
}
