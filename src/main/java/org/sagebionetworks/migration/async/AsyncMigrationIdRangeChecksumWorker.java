package org.sagebionetworks.migration.async;

import org.sagebionetworks.client.SynapseAdminClient;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.migration.AdminResponse;
import org.sagebionetworks.repo.model.migration.AsyncMigrationRangeChecksumRequest;
import org.sagebionetworks.repo.model.migration.MigrationRangeChecksum;
import org.sagebionetworks.repo.model.migration.MigrationType;

import java.util.concurrent.Callable;

public class AsyncMigrationIdRangeChecksumWorker implements Callable<MigrationRangeChecksum> {

	private SynapseAdminClient conn;
	private MigrationType type;
	private String salt;
	private long maxId;
	private long minId;
	private long timeoutMS;

	public AsyncMigrationIdRangeChecksumWorker(SynapseAdminClient conn,
											   MigrationType type,
											   String salt,
											   long minId,
											   long maxId,
											   long timeoutMS) {
		this.conn = conn;
		this.type = type;
		this.salt = salt;
		this.minId = minId;
		this.maxId = maxId;
		this.timeoutMS = timeoutMS;

	}

	@Override
	public MigrationRangeChecksum call() throws InterruptedException {
		MigrationRangeChecksum res = null;
		try {
			res = conn.getChecksumForIdRange(type, salt, minId, maxId);
			return res;
		} catch (SynapseException e) {
			AsyncMigrationRangeChecksumRequest req = new AsyncMigrationRangeChecksumRequest();
			req.setType(type.name());
			req.setSalt(salt);
			req.setMinId(minId);
			req.setMaxId(maxId);
			AsyncMigrationRequestExecutor worker = new AsyncMigrationRequestExecutor(conn, req, timeoutMS);
			AdminResponse resp = worker.execute();
			res = (MigrationRangeChecksum)resp;
			return res;
		}
	}
}
