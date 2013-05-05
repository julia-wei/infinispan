package org.infinispan.monitoring.util;

import org.apache.commons.lang.builder.ToStringBuilder;

public class MemoryUser {
    
    Long memory;
    String objectType;
    
    public Long getMemory() {
        return memory;
    }
    public void setMemory(Long memory) {
        this.memory = memory;
    }
    public String getObjectType() {
        return objectType;
    }
    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }
    
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
