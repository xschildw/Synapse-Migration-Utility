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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = ((prime*result)+((srcResult == null)? 0 :srcResult.hashCode()));
		result = ((prime*result)+((destResult == null)? 0 :destResult.hashCode()));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass()!= obj.getClass()) {
			return false;
		}
		ConcurrentExecutionResult<T> other = ((ConcurrentExecutionResult<T>) obj);
		if (srcResult == null) {
			if (other.srcResult != null) {
				return false;
			}
		} else {
			if (! srcResult.equals(other.srcResult)) {
				return false;
			}
		}
		if (destResult == null) {
			if (other.destResult != null) {
				return false;
			}
		} else {
			if (! destResult.equals(other.destResult)) {
				return false;
			}
		}
		return true;
	}
}
