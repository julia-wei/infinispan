package org.infinispan.monitoring.domain;

import org.apache.commons.lang.builder.ToStringBuilder;

public class RecordCount extends Node {
	
	private int count;
	private String cacheName;
	private String simpleCacheName; // for display purposes only.
	/*
	 * Each CacheManager should have its own JMX Domain. 
	 * The JMX Domain is configured through the "jmxDomain"
	 * attribute of the "globalJmxStatistics" element in the ISPN
	 * configuration. The "globalJmxStatistics" element should appear
	 * within the "global" element, with the "enabled" attribute
	 * set to true.
	 */
	private String jmxDomain;
	private String numOwners;

	public String getNumOwners() {
        return numOwners;
    }

    public void setNumOwners(String numOwners) {
        this.numOwners = numOwners;
    }

    public String getJmxDomain() {
		return jmxDomain;
	}

	public void setJmxDomain(String jmxDomain) {
		this.jmxDomain = jmxDomain;
	}

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public String getSimpleCacheName() {
        return simpleCacheName;
    }

    public void setSimpleCachNamee(String simpleCacheName) {
        this.simpleCacheName = simpleCacheName;
    }

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
