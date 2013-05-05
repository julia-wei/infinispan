package org.infinispan.monitoring.util;

import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;

public class RemoteJMXConnection {
	
	MBeanServerConnection conn;
	JMXConnector jmxConnector;
	Set<ObjectName> mbeans;
	
	public JMXConnector getJmxConnector() {
		return jmxConnector;
	}

	public void setJmxConnector(JMXConnector jmxConnector) {
		this.jmxConnector = jmxConnector;
	}
	
	public RemoteJMXConnection() { }

	public RemoteJMXConnection(MBeanServerConnection conn, JMXConnector jmxConnector, Set<ObjectName> mbeans) {
		this.conn = conn;
		this.jmxConnector = jmxConnector;
		this.mbeans = mbeans;
	}
	
	public MBeanServerConnection getConn() {
		return conn;
	}
	public void setConn(MBeanServerConnection conn) {
		this.conn = conn;
	}
	public Set<ObjectName> getMbeans() {
		return mbeans;
	}
	public void setMbeans(Set<ObjectName> mbeans) {
		this.mbeans = mbeans;
	}
}
