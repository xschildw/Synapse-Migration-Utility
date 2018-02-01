package org.sagebionetworks.migration;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.migration.async.AsynchronousJobExecutor;
import org.sagebionetworks.migration.async.ResultPair;
import org.sagebionetworks.migration.factory.SynapseClientFactory;
import org.sagebionetworks.repo.model.migration.AsyncMigrationTypeCountsRequest;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;
import org.sagebionetworks.repo.model.migration.MigrationTypeCounts;
import org.sagebionetworks.repo.model.migration.MigrationTypeNames;

import com.amazonaws.services.sqs.model.UnsupportedOperationException;
import com.google.inject.Inject;

public class TypeServiceImpl implements TypeService {
	
	SynapseAdminClient sourceClient;
	SynapseAdminClient destinationClient;
	AsynchronousJobExecutor asynchronousJobExecutor;
	
	@Inject
	public TypeServiceImpl(SynapseClientFactory clientFactory) {
		this.sourceClient = clientFactory.getSourceClient();
		this.destinationClient = clientFactory.getDestinationClient();
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.migration.TypeService#getAllCommonMigrationTypes()
	 */
	@Override
	public List<MigrationType> getAllCommonMigrationTypes() {
		try {
			MigrationTypeNames srcMigrationTypeNames = sourceClient.getMigrationTypeNames();
			MigrationTypeNames destMigrationTypeNames = destinationClient.getMigrationTypeNames();
			return getMigrationTypeIntersection(srcMigrationTypeNames.getList(), destMigrationTypeNames.getList());
		} catch (SynapseException e) {
			throw new AsyncMigrationException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.migration.TypeService#getCommonPrimaryMigrationTypes()
	 */
	@Override
	public List<MigrationType> getCommonPrimaryMigrationTypes() {
		try {
			MigrationTypeNames srcMigrationTypeNames = sourceClient.getPrimaryTypeNames();
			MigrationTypeNames destMigrationTypeNames = destinationClient.getPrimaryTypeNames();
			return getMigrationTypeIntersection(srcMigrationTypeNames.getList(), destMigrationTypeNames.getList());
		} catch (SynapseException e) {
			throw new AsyncMigrationException(e);
		}
	}

	/**
	 * Given the name of the types from both the source and destination, get the intersection of the types
	 * found in both.
	 * @param sourceNames
	 * @param destinationNamess
	 * @return
	 */
	List<MigrationType> getMigrationTypeIntersection(List<String> sourceNames, List<String> destinationNamess) {
		List<String> nameIntersection = getNameIntersection(sourceNames, destinationNamess);
		List<MigrationType> commonTypes = new LinkedList<MigrationType>();
		for (String name: nameIntersection) {
			commonTypes.add(MigrationType.valueOf(name));
		}
		return commonTypes;
	}

	/**
	 * The intersection of the names that exists on both the source and destination.
	 * 
	 * @param sourceNames
	 * @param destinationNames
	 * @return
	 */
	List<String> getNameIntersection(List<String> sourceNames, List<String> destinationNames) {
		Set<String> sourceNameSet = new HashSet<>(sourceNames);
		List<String> commonNames = new LinkedList<String>();
		for (String typeName: destinationNames) {
			// Only keep the destination names that are in the source
			if (sourceNameSet.contains(typeName)) {
				commonNames.add(typeName);
			}
		}
		return commonNames;
	}
	
	@Override
	public ResultPair<List<MigrationTypeCount>> getMigrationTypeCounts(List<MigrationType> migrationTypes)
			throws AsyncMigrationException {
		// request for both source and destination.
		AsyncMigrationTypeCountsRequest request = new AsyncMigrationTypeCountsRequest();
		request.setTypes(migrationTypes);
		ResultPair<MigrationTypeCounts> bundleResult = asynchronousJobExecutor.executeSourceAndDestinationJob(request, MigrationTypeCounts.class);
		ResultPair<List<MigrationTypeCount>> result = new ResultPair<List<MigrationTypeCount>>();
		result.setSourceResult(bundleResult.getSourceResult().getList());
		result.setDestinationResult(bundleResult.getDestinationResult().getList());
		return result;
	}

	@Override
	public ResultPair<List<MigrationTypeChecksum>> getFullTableChecksums(List<MigrationType> migrationTypes) {
		throw new UnsupportedOperationException("Need to add support for this");
	}
}
