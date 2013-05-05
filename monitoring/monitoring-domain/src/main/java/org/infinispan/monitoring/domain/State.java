package org.infinispan.monitoring.domain;

import org.apache.commons.lang.builder.ToStringBuilder;

public class State extends Node {
	private String state;
	private String cacheName;
	private String simpleCacheName; // for display purposes only
	private Boolean joinComplete;
	private Boolean stateTransferInProgress;
	private String jmxDomain;
	private String numOwners;
	
	public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public String getSimpleCacheName() {
        return simpleCacheName;
    }

    public void setSimpleCacheName(String simpleCacheName) {
        this.simpleCacheName = simpleCacheName;
    }

    public String getNumOwners() {
        return numOwners;
    }

    public void setNumOwners(String numOwners) {
        this.numOwners = numOwners;
    }

    public Boolean getJoinComplete() {
		return joinComplete;
	}

	public void setJoinComplete(Boolean joinComplete) {
		this.joinComplete = joinComplete;
	}

	public Boolean getStateTransferInProgress() {
		return stateTransferInProgress;
	}

	public void setStateTransferInProgress(Boolean stateTransferInProgress) {
		this.stateTransferInProgress = stateTransferInProgress;
	}

	public String getJmxDomain() {
		return jmxDomain;
	}

	public void setJmxDomain(String jmxDomain) {
		this.jmxDomain = jmxDomain;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
