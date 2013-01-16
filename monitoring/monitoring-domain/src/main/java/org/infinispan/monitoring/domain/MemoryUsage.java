package org.infinispan.monitoring.domain;

import org.apache.commons.lang.builder.ToStringBuilder;

public class MemoryUsage extends Node {
    
    private Long totalMemory;
    private Long objectTypeMemory;
    private String objectType;
    private String cacheName;
    private String simpleCacheName; // for display purposes only
    private String jmxDomain;
    
    public Long getTotalMemory() {
        return totalMemory;
    }
    public void setTotalMemory(Long totalMemory) {
        this.totalMemory = totalMemory;
    }
    public Long getObjectTypeMemory() {
        return objectTypeMemory;
    }
    public void setObjectTypeMemory(Long objectTypeMemory) {
        this.objectTypeMemory = objectTypeMemory;
    }
    public String getObjectType() {
        return objectType;
    }
    public void setObjectType(String objectType) {
        this.objectType = objectType;
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
