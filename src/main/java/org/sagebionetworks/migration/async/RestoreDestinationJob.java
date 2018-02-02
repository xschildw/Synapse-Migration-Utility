package org.sagebionetworks.migration.async;

import org.sagebionetworks.repo.model.migration.MigrationType;

/**
 * Restore job to run on the destination.
 */
public class RestoreDestinationJob implements DestinationJob {

	MigrationType migrationType;
	String backupFileKey;
	
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
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((backupFileKey == null) ? 0 : backupFileKey.hashCode());
		result = prime * result + ((migrationType == null) ? 0 : migrationType.hashCode());
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
		if (migrationType != other.migrationType)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "RestoreDestinationJob [migrationType=" + migrationType + ", backupFileKey=" + backupFileKey + "]";
	}
	
}
