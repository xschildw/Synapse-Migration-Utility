package org.sagebionetworks.migration.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.migration.config.Configuration;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;
import org.sagebionetworks.repo.model.migration.MigrationTypeCounts;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;

public class ToolMigrationUtils {

	/**
	 * Build the metadata for the primary types to migrateWithRetry
	 *
	 * @param srcCounts
	 * @param destCounts
	 * @param typesToMigrate
	 * @return
	 */
	public static List<TypeToMigrateMetadata> buildTypeToMigrateMetadata(
		List<MigrationTypeCount> srcCounts, List<MigrationTypeCount> destCounts,
		List<MigrationType> typesToMigrate) {
		if (srcCounts == null) throw new IllegalArgumentException("srcCounts cannot be null.");
		if (destCounts == null) throw new IllegalArgumentException("destCounts cannot be null.");
		if (typesToMigrate == null) throw new IllegalArgumentException("typesToMigrate cannot be null.");
		
		List<TypeToMigrateMetadata> l = new LinkedList<TypeToMigrateMetadata>();
		for (MigrationType t: typesToMigrate) {
			MigrationTypeCount srcMtc = findMetadata(srcCounts, t);
			if (srcMtc == null) {
				throw new RuntimeException("Could not find type " + t.name() + " in source migrationTypeCounts");
			}
			MigrationTypeCount destMtc = findMetadata(destCounts, t);
			if (destMtc == null) {
				throw new RuntimeException("Could not find type " + t.name() + " in destination migrationTypeCounts");
			}
			TypeToMigrateMetadata data = new TypeToMigrateMetadata(t, srcMtc.getMinid(), srcMtc.getMaxid(), srcMtc.getCount(), destMtc.getMinid(), destMtc.getMaxid(), destMtc.getCount());
			l.add(data);
		}
		return l;
	}
	
	private static MigrationTypeCount findMetadata(List<MigrationTypeCount> tCounts, MigrationType t) {
		MigrationTypeCount tc = null;
		for (MigrationTypeCount c: tCounts) {
			if (c.getType().equals(t)) {
				tc = c;
				break;
			}
		}
		return tc;
	}
	
	public static List<MigrationTypeCountDiff> getMigrationTypeCountDiffs(List<MigrationTypeCount> srcCounts, List<MigrationTypeCount> destCounts) {
		List<MigrationTypeCountDiff> result = new LinkedList<MigrationTypeCountDiff>();
		Map<MigrationType, Long> mapSrcCounts = new HashMap<MigrationType, Long>();
		for (MigrationTypeCount sMtc: srcCounts) {
			mapSrcCounts.put(sMtc.getType(), sMtc.getCount());
		}
		// All migration types of source should be at destination
		// Note: unused src migration types are covered, they're not in destination results
		for (MigrationTypeCount mtc: destCounts) {
			MigrationTypeCountDiff outcome = 	new MigrationTypeCountDiff(mtc.getType(), (mapSrcCounts.containsKey(mtc.getType()) ? mapSrcCounts.get(mtc.getType()) : null), mtc.getCount());
			result.add(outcome);
		}
		return result;
	}

	public static int actualMaximumNumberOfDestinationJobs(int tryNum, Configuration config) {
		return (tryNum == 0 ? config.getMaximumNumberOfDestinationJobs() : 1);
	}

}
