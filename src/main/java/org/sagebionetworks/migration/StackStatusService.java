package org.sagebionetworks.migration;

/**
 * Abstraction for changing a stack's status.
 *
 */
public interface StackStatusService {

	/**
	 * Set the destination stack to READ-ONLY.
	 */
	void setDestinationReadOnly();

	/**
	 * Set the destination stack to READ-WRITE.
	 */
	void setDestinationReadWrite();

	/**
	 * Is the source in read-only mode?
	 * @return
	 */
	boolean isSourceReadOnly();

}
