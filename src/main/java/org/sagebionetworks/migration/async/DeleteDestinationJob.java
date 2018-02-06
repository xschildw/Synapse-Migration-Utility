package org.sagebionetworks.migration.async;

import java.util.List;

import org.sagebionetworks.repo.model.migration.MigrationType;

/**
 * Job to delete rows from the destination.
 *
 */
public class DeleteDestinationJob implements DestinationJob {

	MigrationType migrationType;
	List<Long> rowIdsToDelete;
	
	@Override
	public MigrationType getMigrationType() {
		return migrationType;
	}

	public void setMigrationType(MigrationType migrationType) {
		this.migrationType = migrationType;
	}

	public List<Long> getRowIdsToDelete() {
		return rowIdsToDelete;
	}

	public void setRowIdsToDelete(List<Long> rowIdsToDelete) {
		this.rowIdsToDelete = rowIdsToDelete;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((migrationType == null) ? 0 : migrationType.hashCode());
		result = prime * result + ((rowIdsToDelete == null) ? 0 : rowIdsToDelete.hashCode());
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
		DeleteDestinationJob other = (DeleteDestinationJob) obj;
		if (migrationType != other.migrationType)
			return false;
		if (rowIdsToDelete == null) {
			if (other.rowIdsToDelete != null)
				return false;
		} else if (!rowIdsToDelete.equals(other.rowIdsToDelete))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "DeleteDestinationJob [migrationType=" + migrationType + ", rowIdsToDelete=" + rowIdsToDelete + "]";
	}

	
}
