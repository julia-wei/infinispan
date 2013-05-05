package org.infinispan.monitoring.util;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JmxHelper {
    private static Log log = LogFactory.getLog(JmxHelper.class);

    private static boolean debug = log.isDebugEnabled();
    private static boolean trace = log.isTraceEnabled();

    // Note that this is the format for the URL used by AS7 and EAP6. Older
    // versions of JBoss use:
    // "service:jmx:rmi:///jndi/rmi://host:port/jmxrmi"
    //protected static String REMOTE_JMX_URL = "service:jmx:remoting-jmx://%s:%s";
    protected static String REMOTE_JMX_URL = "service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi";
    protected static String JMX_USER = null; //"ispn";
    protected static String JMX_PASS = null; //"ispn123";

    protected static MBeanServer server = null;
    protected static JMXConnector jmxConnector = null;
    protected RemoteJMXConnection remoteConnection = null;

    public JmxHelper() {
    }

    /*
     * Searches for MBeans that are co-located on the same application server as
     * the calling application. They must match the provided pattern, and adhere
     * to the parameters provided in the query expression. A null query
     * expression may be provided, but the pattern is required.
     */
    public Set<ObjectName> getLocalMBeans(String pattern, QueryExp query)
            throws Exception {
        Set<ObjectName> mbeans = null;

        if (debug)
            log.debug("Querying for JMX beans matching pattern: '" + pattern + "'.");

        mbeans = getLocalMBeanServer().queryNames(new ObjectName(pattern), query);

        if (trace) {
            Iterator<ObjectName> it = mbeans.iterator();
            if (!it.hasNext())
                log.trace("Got empty set back after executing mbean query.");
            while (it.hasNext()) {
                log.trace("Got mbean: '" + it.next().toString() + "'.");
            }
        }

        return mbeans;
    }

    public RemoteJMXConnection getRemoteMBeans(String host, String port,
            String pattern, QueryExp query) throws Exception {
        Set<ObjectName> mbeans = null;
        RemoteJMXConnection ret = null;

        if (debug)
            log.debug("Querying for JMX beans matching pattern: '" + pattern + "' from remote server '" + host + ":" + port + "'.");

        ret = getRemoteMBeanServerConnection(host, port);
        mbeans = ret.getConn().queryNames(new ObjectName(pattern), query);
        if(trace)
            log.trace("Query returned "+mbeans.size()+" mbeans.");
        ret.setMbeans(mbeans);

        if (trace) {
            Iterator<ObjectName> it = mbeans.iterator();
            if (it.hasNext())
                log.trace("Got empty set back after executing mbean query.");
            while (it.hasNext()) {
                log.trace("Got mbean: '" + it.next().toString() + "'.");
            }
        }

        return ret;
    }

    /*
     * Invokes an operation located in an MBean that is co-located on the same
     * application server as the calling application.
     */
    public Object invokeLocalMBeanOperation(ObjectName objName, String op)
            throws Exception {
        Object ret = null;

        if (debug)
            log.debug("Invoking '" + op + "' on mbean " + objName.toString());

        ret = getLocalMBeanServer().invoke(objName, op, null, new String[0]);

        return ret;
    }

    /*
     * Gets an attribute located in an MBean that is co-located on the same
     * application server as the calling application.
     */
    public Object getLocalMBeanAttribute(ObjectName objName, String attr)
            throws Exception {
        Object ret = null;

        if (debug)
            log.debug("Getting attribute '" + attr + "' from local mbean " + objName.toString());

        ret = getLocalMBeanServer().getAttribute(objName, attr);

        return ret;
    }

    public Object getRemoteMBeanAttribute(MBeanServerConnection conn,
            ObjectName objName, String attr) throws Exception {
        Object ret = null;

        if (debug)
            log.debug("Getting attribute '" + attr + "' from remote mbean " + objName.toString());

        if (conn == null)
            throw new RuntimeException("Remote JMX connection is null.");
        if (objName == null)
            throw new RuntimeException("JMX bean ObjectName is null.");
        if (attr == null)
            throw new RuntimeException("JMX attribute to fetch is null.");

        ret = conn.getAttribute(objName, attr);

        return ret;
    }

    /*
     * Returns the local MBean server, which provides an interface to JMX beans
     * exposed on the same application server as the calling applicatoin.
     */
    public MBeanServer getLocalMBeanServer() {
        if (server == null) {
            if (trace)
                log.trace("Fetching local MBean server.");
            server = ManagementFactory.getPlatformMBeanServer();
        }

        if (trace)
            log.trace("Returning local MBean server.");
        return server;
    }

    public RemoteJMXConnection getRemoteMBeanServerConnection(String host,
            String port) throws Exception {
        String url = System.getProperty("jmx.service.url",
                String.format(REMOTE_JMX_URL, host, port));

        JMXServiceURL jmxServiceURL = new JMXServiceURL(url);
        if (!StringUtils.isBlank(JMX_USER) && !StringUtils.isBlank(JMX_PASS)) {
            if(debug)
                log.debug("Using JMX service url '"+url+"' and providing credentials.");
            Map env = new HashMap();
            String[] auth = new String[2];
            auth[0] = JMX_USER;
            auth[1] = JMX_PASS;
            env.put(JMXConnector.CREDENTIALS, auth);
            jmxConnector = JMXConnectorFactory.connect(jmxServiceURL, env);
        } else {
            if(debug)
                log.debug("Using JMX service url '"+url+"' with no credentials.");
            jmxConnector = JMXConnectorFactory.connect(jmxServiceURL);
        }
        if(trace)
            log.trace("Getting remote MBeanServerConnection.");
        MBeanServerConnection connection = jmxConnector.getMBeanServerConnection();
        if(trace)
            log.trace("Got remote MBeanServerConnection.");

        remoteConnection = new RemoteJMXConnection();
        remoteConnection.setConn(connection);
        remoteConnection.setJmxConnector(jmxConnector);

        return remoteConnection;
    }

    /*
     * public RemoteJMXConnection getKludgeRemoteMBeanServerConnection(String
     * host, String port) throws Exception { RemoteJMXConnection ret = new
     * RemoteJMXConnection(); String url = System.getProperty("jmx.service.url",
     * "service:jmx:rmi:///jndi/rmi://" + host + ":" + port+"/jmxrmi");
     * JMXServiceURL jmxServiceURL = new JMXServiceURL(url); //Map<String,
     * String> env = new HashMap<String, String>();
     * //env.put(InitialContext.INITIAL_CONTEXT_FACTORY,
     * RMIContextFactory.class.getName()); JMXConnector jmxConnector =
     * JMXConnectorFactory.connect(jmxServiceURL, null); MBeanServerConnection
     * connection = jmxConnector .getMBeanServerConnection();
     * ret.setConn(connection); ret.setJmxConnector(jmxConnector); return ret; }
     */
}
