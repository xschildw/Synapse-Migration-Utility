package org.sagebionetworks.migration.delta;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.migration.async.ResultPair;
import org.sagebionetworks.migration.async.ConcurrentMigrationIdRangeChecksumsExecutor;
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.repo.model.migration.AsyncMigrationRangeChecksumRequest;
import org.sagebionetworks.repo.model.migration.MigrationRangeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.schema.adapter.JSONObjectAdapterException;
import org.sagebionetworks.migration.utils.TypeToMigrateMetadata;
import org.sagebionetworks.migration.async.AsyncMigrationRequestExecutor;

public class DeltaFinder {

	static private Logger logger = LogManager.getLogger(DeltaFinder.class);

	TypeToMigrateMetadata typeToMigrateMeta;
	String salt;
	Long batchSize;
	ConcurrentMigrationIdRangeChecksumsExecutor concurrentIdRangeChecksumExecutor;
	
	public DeltaFinder(TypeToMigrateMetadata tm,
			String salt,
			Long bSize,
					   ConcurrentMigrationIdRangeChecksumsExecutor executor) {
		typeToMigrateMeta = tm;
		this.salt = salt;
		batchSize = bSize;
		concurrentIdRangeChecksumExecutor = executor;
	}

	public DeltaRanges findDeltaRanges() throws SynapseException, JSONObjectAdapterException, InterruptedException {
		DeltaRanges deltas = new DeltaRanges();
		deltas.setMigrationType(typeToMigrateMeta.getType());
		List<IdRange> insRanges = new LinkedList<IdRange>();
		List<IdRange> updRanges = new LinkedList<IdRange>();
		List<IdRange> delRanges = new LinkedList<IdRange>();
		
		// Source is empty
		if (typeToMigrateMeta.getSrcMinId() == null) {
			// Delete everything at destination if not empty
			if (typeToMigrateMeta.getDestMinId() != null) {
				IdRange r = new IdRange(typeToMigrateMeta.getDestMinId(), typeToMigrateMeta.getDestMaxId());
				delRanges.add(r);
				logger.debug("Source is empty for " + typeToMigrateMeta.getType() + ", added range " + r + " to delRanges.");
			}
		} else { // Source is not empty
			// Insert everything from destination if empty
			if (typeToMigrateMeta.getDestMinId() == null) {
				IdRange r = new IdRange(typeToMigrateMeta.getSrcMinId(), typeToMigrateMeta.getSrcMaxId());
				insRanges.add(r);
				logger.debug("Destination is empty for " + typeToMigrateMeta.getType() + ", added range " + r + " to insRanges.");
			} else { // Normal case
				Long updatesMinId = Math.max(typeToMigrateMeta.getSrcMinId(), typeToMigrateMeta.getDestMinId());
				Long updatesMaxId = Math.min(typeToMigrateMeta.getSrcMaxId(), typeToMigrateMeta.getDestMaxId());
				logger.debug("Update box from " + updatesMinId + " to " + updatesMaxId);
				
				if (updatesMinId > updatesMaxId) { // Disjoint
					IdRange r = new IdRange(typeToMigrateMeta.getSrcMinId(), typeToMigrateMeta.getSrcMaxId());
					insRanges.add(r);
					logger.debug("Added range " + r + " to insRanges.");
					r = new IdRange(typeToMigrateMeta.getDestMinId(), typeToMigrateMeta.getDestMaxId());
					delRanges.add(r);
					logger.debug("Added range " + r + " to delRanges.");
				} else {
					// Inserts and deletes ranges (these are ranges that do not overlap between source and destination
					// so either insert or delete depending where they occur
					if (typeToMigrateMeta.getSrcMinId() < updatesMinId) {
						IdRange r = new IdRange(typeToMigrateMeta.getSrcMinId(), updatesMinId-1);
						insRanges.add(r);
						logger.debug("Added range " + r + " to insRanges.");
					}
					if (typeToMigrateMeta.getSrcMaxId() > updatesMaxId) {
						IdRange r = new IdRange(updatesMaxId+1, typeToMigrateMeta.getSrcMaxId());
						insRanges.add(r);
						logger.debug("Added range " + r + " to insRanges.");
					}
					if (typeToMigrateMeta.getDestMinId() < updatesMinId) {
						IdRange r = new IdRange(typeToMigrateMeta.getDestMinId(), updatesMinId-1);
						delRanges.add(r);
						logger.debug("Added range " + r + " to delRanges.");
					}
					if (typeToMigrateMeta.getDestMaxId() > updatesMaxId) {
						IdRange r = new IdRange(updatesMaxId+1, typeToMigrateMeta.getDestMaxId());
						delRanges.add(r);
						logger.debug("Added range " + r + " to delRanges.");
					}
					
					// Update ranges
					updRanges.addAll(findUpdDeltaRanges(typeToMigrateMeta.getType(), salt, updatesMinId, updatesMaxId, batchSize));
				}

			}
		}
		
		// Update ranges
		
		deltas.setInsRanges(insRanges);
		deltas.setUpdRanges(updRanges);
		deltas.setDelRanges(delRanges);
		return deltas;
	}
	
	private List<IdRange> findUpdDeltaRanges(MigrationType type, String salt, long minId, long maxId, long batchSize) throws SynapseException, JSONObjectAdapterException, InterruptedException {
		List<IdRange> l = new LinkedList<IdRange>();
		ResultPair<MigrationRangeChecksum> rangeChecksums = this.concurrentIdRangeChecksumExecutor.getIdRangeChecksums(type, salt, minId, maxId);
		MigrationRangeChecksum srcCrc32 = rangeChecksums.getSourceResult();
		MigrationRangeChecksum destCrc32 = rangeChecksums.getDestinationResult();
		//log.info("Computed range checksums from " + minId + " to " + maxId + ": (" + srcCrc32.getChecksum() + ", " + destCrc32.getChecksum() + ").");
		if (srcCrc32.equals(destCrc32)) {
			return l;
		} else {
			if (maxId - minId < batchSize) {
				IdRange r = new IdRange(minId, maxId);
				l.add(r);
				return l;
			} else { // Split
				long minId1 = minId;
				long maxId1 = (minId+maxId)/2;
				long minId2 = maxId1+1;
				long maxId2 = maxId;
				l.addAll(findUpdDeltaRanges(type, salt, minId1, maxId1, batchSize));
				l.addAll(findUpdDeltaRanges(type, salt, minId2, maxId2, batchSize));
				return l;
			}
		}
	}
}
