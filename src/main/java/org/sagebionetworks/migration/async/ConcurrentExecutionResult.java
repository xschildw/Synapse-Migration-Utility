package org.sagebionetworks.migration.async;

import org.sagebionetworks.repo.model.migration.AdminResponse;

/**
 * A container for a pair of results (any type)
 */
public class ConcurrentExecutionResult<T> {

	private T srcResponse;
	private T destResponse;

	public void setSourceResponse(T resp) {
		this.srcResponse = resp;
	}

	public T getSourceResponse() {
		return this.srcResponse;
	}

	public void setDestinationResponse(T resp) {
		this.destResponse = resp;
	}

	public T getDestinationResponse() {
		return this.destResponse;
	}
}
