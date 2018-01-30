package org.sagebionetworks.migration.config;

/**
 * Holds all of the information needed to establish a Synapse Connection
 *
 */
public class SynapseConnectionInfo {
	
	/**
	 * The only public constructor.
	 * @param authenticationEndPoint
	 * @param repositoryEndPoint
	 * @param adminUsername
	 * @param adminPassword
	 */
	public SynapseConnectionInfo(String authenticationEndPoint,
			String repositoryEndPoint,
			String userName,
			String APIKey
			) {
		super();
		this.authenticationEndPoint = authenticationEndPoint;
		this.repositoryEndPoint = repositoryEndPoint;
		this.APIKey = APIKey;
		this.userName = userName;
	}
	
	private String authenticationEndPoint;
	private String repositoryEndPoint;
	private String APIKey;
	private String userName;
	
	
	public String getAuthenticationEndPoint() {
		return authenticationEndPoint;
	}
	public String getRepositoryEndPoint() {
		return repositoryEndPoint;
	}
	public String getApiKey() {
		return APIKey;
	}
	public String getUserName() {
		return userName;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((APIKey == null) ? 0 : APIKey.hashCode());
		result = prime * result + ((authenticationEndPoint == null) ? 0 : authenticationEndPoint.hashCode());
		result = prime * result + ((repositoryEndPoint == null) ? 0 : repositoryEndPoint.hashCode());
		result = prime * result + ((userName == null) ? 0 : userName.hashCode());
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
		SynapseConnectionInfo other = (SynapseConnectionInfo) obj;
		if (APIKey == null) {
			if (other.APIKey != null)
				return false;
		} else if (!APIKey.equals(other.APIKey))
			return false;
		if (authenticationEndPoint == null) {
			if (other.authenticationEndPoint != null)
				return false;
		} else if (!authenticationEndPoint.equals(other.authenticationEndPoint))
			return false;
		if (repositoryEndPoint == null) {
			if (other.repositoryEndPoint != null)
				return false;
		} else if (!repositoryEndPoint.equals(other.repositoryEndPoint))
			return false;
		if (userName == null) {
			if (other.userName != null)
				return false;
		} else if (!userName.equals(other.userName))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "SynapseConnectionInfo [authenticationEndPoint=" + authenticationEndPoint + ", repositoryEndPoint="
				+ repositoryEndPoint + ", APIKey=" + APIKey + ", userName=" + userName + "]";
	}


}
