package org.infinispan.monitoring.domain;

import org.apache.commons.lang.builder.ToStringBuilder;

public class PhysicalAddress {
	
	private String host;
	private String port;
	/*
	 * Each CacheManager should have its own JMX Domain. 
	 * The JMX Domain is configured through the "jmxDomain"
	 * attribute of the "globalJmxStatistics" element in the ISPN
	 * configuration. The "globalJmxStatistics" element should appear
	 * within the "global" element, with the "enabled" attribute
	 * set to true.
	 */
	private String jmxDomain;
	
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getJmxDomain() {
		return jmxDomain;
	}
	public void setJmxDomain(String jmxDomain) {
		this.jmxDomain = jmxDomain;
	}
	
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
