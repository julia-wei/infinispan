package org.infinispan.monitoring.domain;

import org.apache.commons.lang.builder.ToStringBuilder;

public class Node {
	
	private String nodeName;

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}
	
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
