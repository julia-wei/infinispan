package org.infinispan.monitoring.listener.memory;

import java.util.concurrent.atomic.AtomicLong;

public class MemoryMgmtEntryDesc implements Comparable<MemoryMgmtEntryDesc> {
    protected static final int BEFORE = -1;
    protected static final int EQUAL = 0;
    protected static final int AFTER = 1;
	
	private String objectType;
	private AtomicLong size = new AtomicLong(0);

	public String getObjectType() {
		return objectType;
	}

	public void setObjectType(String objectType) {
		this.objectType = objectType;
	}

	public AtomicLong getSize() {
		return size;
	}

	public void setSize(AtomicLong size) {
		this.size = size;
	}

	@Override
	public int compareTo(MemoryMgmtEntryDesc entry) {
		
		int result = EQUAL;
		
		// Sort in descending order
		if(this.size.get() > entry.getSize().get()) result = BEFORE;
		if(this.size.get() < entry.getSize().get()) result = AFTER;
		
		return result;
	}

}
