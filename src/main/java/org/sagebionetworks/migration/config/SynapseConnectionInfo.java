package org.sagebionetworks.migration.config;

import java.util.Objects;

/**
 * Holds all of the information needed to establish a Synapse Connection
 *
 */
public class SynapseConnectionInfo {
	
	/**
	 * The only public constructor.
	 * @param authenticationEndPoint
	 * @param repositoryEndPoint
	 * @param serviceKey
	 * @param serviceSecret
	 */
	public SynapseConnectionInfo(String authenticationEndPoint,
			String repositoryEndPoint,
			String serviceKey,
			String serviceSecret
			) {
		super();
		this.authenticationEndPoint = authenticationEndPoint;
		this.repositoryEndPoint = repositoryEndPoint;
		this.serviceKey = serviceKey;
		this.serviceSecret = serviceSecret;
	}
	
	private String authenticationEndPoint;
	private String repositoryEndPoint;
	private String serviceKey;
	private String serviceSecret;
	
	public String getAuthenticationEndPoint() {
		return authenticationEndPoint;
	}
	
	public String getRepositoryEndPoint() {
		return repositoryEndPoint;
	}

	public String getServiceKey() {
		return serviceKey;
	}
	
	public String getServiceSecret() {
		return serviceSecret;
	}

	@Override
	public int hashCode() {
		return Objects.hash(authenticationEndPoint, repositoryEndPoint, serviceKey, serviceSecret);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		SynapseConnectionInfo other = (SynapseConnectionInfo) obj;
		return Objects.equals(authenticationEndPoint, other.authenticationEndPoint)
				&& Objects.equals(repositoryEndPoint, other.repositoryEndPoint)
				&& Objects.equals(serviceKey, other.serviceKey) && Objects.equals(serviceSecret, other.serviceSecret);
	}

	@Override
	public String toString() {
		return "SynapseConnectionInfo [authenticationEndPoint=" + authenticationEndPoint + ", repositoryEndPoint="
				+ repositoryEndPoint + ", serviceKey=" + serviceKey + "]";
	}

}
