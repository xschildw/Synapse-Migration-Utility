package org.sagebionetworks.migration.async;

import org.sagebionetworks.repo.model.migration.MigrationType;

/**
 * Restore job to run on the destination.
 */
public class RestoreDestinationJob implements DestinationJob {

	MigrationType migrationType;
	String backupFileKey;
	Long minimumId;
	Long maximumId;
	
	public RestoreDestinationJob(MigrationType migrationType, String backupFileKey) {
		super();
		this.migrationType = migrationType;
		this.backupFileKey = backupFileKey;
	}
	
	public RestoreDestinationJob(MigrationType migrationType, String backupFileKey, Long minimumId, Long maximumId) {
		super();
		this.migrationType = migrationType;
		this.backupFileKey = backupFileKey;
		this.minimumId = minimumId;
		this.maximumId = maximumId;
	}

	@Override
	public MigrationType getMigrationType() {
		return migrationType;
	}
	public void setMigrationType(MigrationType migrationType) {
		this.migrationType = migrationType;
	}
	public String getBackupFileKey() {
		return backupFileKey;
	}
	public void setBackupFileKey(String backupFileKey) {
		this.backupFileKey = backupFileKey;
	}
	public Long getMinimumId() {
		return minimumId;
	}
	public Long getMaximumId() {
		return maximumId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((backupFileKey == null) ? 0 : backupFileKey.hashCode());
		result = prime * result + ((maximumId == null) ? 0 : maximumId.hashCode());
		result = prime * result + ((migrationType == null) ? 0 : migrationType.hashCode());
		result = prime * result + ((minimumId == null) ? 0 : minimumId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RestoreDestinationJob other = (RestoreDestinationJob) obj;
		if (backupFileKey == null) {
			if (other.backupFileKey != null)
				return false;
		} else if (!backupFileKey.equals(other.backupFileKey))
			return false;
		if (maximumId == null) {
			if (other.maximumId != null)
				return false;
		} else if (!maximumId.equals(other.maximumId))
			return false;
		if (migrationType != other.migrationType)
			return false;
		if (minimumId == null) {
			if (other.minimumId != null)
				return false;
		} else if (!minimumId.equals(other.minimumId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "RestoreDestinationJob [migrationType=" + migrationType + ", backupFileKey=" + backupFileKey
				+ ", minimumId=" + minimumId + ", maximumId=" + maximumId + "]";
	}
	
}
