package org.sagebionetworks.migration.async;

/**
 * A container for a pair of results (any type)
 */
public class ConcurrentExecutionResult<T> {

	private T srcResult;
	private T destResult;

	public void setSourceResult(T resp) {
		this.srcResult = resp;
	}

	public T getSourceResult() {
		return this.srcResult;
	}

	public void setDestinationResult(T resp) {
		this.destResult = resp;
	}

	public T getDestinationResult() {
		return this.destResult;
	}
}
