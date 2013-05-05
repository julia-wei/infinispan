package org.infinispan.monitoring.listener.memory;

import java.util.concurrent.atomic.AtomicLong;

public class MemoryUsageKeyEntry {
    
    private Object key;
    private String type;
    private AtomicLong size = new AtomicLong(0);
    
    public MemoryUsageKeyEntry(Object key, String type, Long size) {
        this.key = key;
        this.type = type;
        this.size.set(size);
    }
    
    public Object getKey() {
        return key;
    }
    public void setKey(Object key) {
        this.key = key;
    }
    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }
    public AtomicLong getSize() {
        return size;
    }
    public void setSize(AtomicLong size) {
        this.size = size;
    }
    public void setSize(Long size) {
        this.size.set(size);
    }

}
