package org.infinispan.monitoring.domain;

public class MessageCount extends Node {

	private long count;
	/*
	 * Each CacheManager should have its own JMX Domain. The JMX Domain is
	 * configured through the "jmxDomain" attribute of the "globalJmxStatistics"
	 * element in the ISPN configuration. The "globalJmxStatistics" element
	 * should appear within the "global" element, with the "enabled" attribute
	 * set to true.
	 */
	private String jmxDomain;

	public String getJmxDomain() {
		return jmxDomain;
	}

	public void setJmxDomain(String jmxDomain) {
		this.jmxDomain = jmxDomain;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

}
