package org.infinispan.monitoring.domain;

import org.apache.commons.lang.builder.ToStringBuilder;

public class TransactionCount extends Node {
	
	public enum TransactionType {
		read("read"),
		write("write"),
		delete("delete");
		
		String type;
		
		private TransactionType(String type) {
			this.type = type;
		}
	}
	
	private String cacheName;
	private String simpleCacheName; // for display purposes only
	/*
	 * Each CacheManager should have its own JMX Domain. 
	 * The JMX Domain is configured through the "jmxDomain"
	 * attribute of the "globalJmxStatistics" element in the ISPN
	 * configuration. The "globalJmxStatistics" element should appear
	 * within the "global" element, with the "enabled" attribute
	 * set to true.
	 */
	private String jmxDomain;
	private String operationType;
	private long time;
	private long count;
	
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
    public void setSimpleCacheName(String simpleCacheName) {
        this.simpleCacheName = simpleCacheName;
    }
	public String getOperationType() {
		return operationType;
	}
	public void setOperationType(String operationType) {
		this.operationType = operationType;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
