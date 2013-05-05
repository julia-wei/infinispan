package org.infinispan.monitoring.util;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import javax.management.ObjectName;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.infinispan.monitoring.domain.GroupMessageCount;
import org.infinispan.monitoring.domain.MemoryUsage;
import org.infinispan.monitoring.domain.MessageCount;
import org.infinispan.monitoring.domain.PhysicalAddress;
import org.infinispan.monitoring.domain.RecordCount;
import org.infinispan.monitoring.domain.State;
import org.infinispan.monitoring.domain.TransactionCount;
import org.infinispan.monitoring.domain.TransactionCount.TransactionType;

public class JmxQueryHelper {
    private static Log log = LogFactory.getLog(JmxQueryHelper.class);
    private static boolean debug = log.isDebugEnabled();
    private static boolean trace = log.isTraceEnabled();

    private static JmxHelper jmxHelper = new JmxHelper();

    protected static final String JMX_DOMAIN = "*";
    protected static final String REMOTE_JMX_PORT = "9999";
    protected static final String GROUP_MEMBER_PROPS = "/group-members.properties";
    protected static final String GROUP_SIZE = "clusterSize";
    protected static final String JOIN_COMPLETE = "joinComplete";
    protected static final String XFER_IN_PROGRESS = "stateTransferInProgress";
    protected static final String NUM_ENTRIES = "numberOfEntries";
    protected static final String NUM_OWNERS = "numOwners";
    protected static final String SIMPLE_CACHE_NAME = "cacheName";
    protected static final String NODE_NAME = "nodeAddress";
    protected static final String CACHE_NAME_KEY = "name";
    protected static final String INCOMING_MESSAGES = "ReceivedMessages";
    protected static final String OUTGOING_MESSAGES = "SentMessages";
    protected static final String INCOMING_BYTES = "ReceivedBytes";
    protected static final String OUTGOING_BYTES = "SentBytes";
    protected static final String TIME = "timeSinceReset";
    protected static final String MEMORY_USAGE = "memoryUsage";
    protected static final String TOTAL_MEMORY = "totalMemory";
    protected static final String PHYSICAL_ADDRESSES = "physicalAddresses";
    protected static final String[] READ_ATTRS = { "hits", "misses" };
    protected static final String[] WRITE_ATTRS = { "stores" };
    protected static final String[] DELETE_ATTRS = { "removeHits",
            "removeMisses" };

    public List<PhysicalAddress> getGroupPhysicalAddresses() {
        String pattern = null;
        String addresses = null;
        Set<ObjectName> names = null;
        ObjectName objName = null;
        List<PhysicalAddress> ret = new ArrayList<PhysicalAddress>();
        List<String> stringAddrs = new ArrayList<String>();
        PhysicalAddress addr = null;
        boolean foundProps = true;

        // CHECK FOR PROPS FILE.
        // Should have format key=value, where "key" is the host of the
        // group member and "value" is the port. This format assumes that the
        // host value is unique.
        Properties props = new Properties();
        try {
            if (trace)
                log.trace("Attempting to load properties file '" + GROUP_MEMBER_PROPS + "'.");
            props.load(JmxQueryHelper.class.getResourceAsStream(GROUP_MEMBER_PROPS));
        } catch (Exception e) {
            if (debug) {
                StringBuffer classpath = new StringBuffer();
                log.debug("Unable to load group memberproperties file '" + GROUP_MEMBER_PROPS + "'. Fetching from JMX and using default management port. Exception is: " + e.getMessage() + ": "
                        + Arrays.asList(e.getStackTrace()).toString());
                e.printStackTrace();
                Enumeration<URL> urls = null;
                try {
                    urls = getClass().getClassLoader().getResources("/");
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                log.debug("Classpath: ");
                while (urls.hasMoreElements()) {
                    classpath.append(urls.nextElement().getFile()).append(System.getProperty("line.separator"));
                }
                log.debug(classpath.toString());
            }
            foundProps = false;
        }

        if (props == null || props.entrySet().size() <= 0) {
            if (trace)
                log.trace("Properties variable is " + (props == null ? "null" : "empty") + " so fetching group member hosts through JMX.");
            foundProps = false;
        }

        // FOUND PROPERTIES FILE
        if (foundProps) {
            if (trace)
                log.trace("Parsing group member host/port properties file.");
            Set<Entry<Object, Object>> entries = props.entrySet();
            for (Entry<Object, Object> e : entries) {
                addr = new PhysicalAddress();
                addr.setHost((String) e.getKey());
                addr.setPort((String) e.getValue());
                if (trace)
                    log.trace("Got group member physical address with host: " + addr.getHost() + " and port: " + addr.getPort());
                /*
                 * No way of knowing remote domains, unless file format changes,
                 * so plug in a default to fetch all domains.
                 */
                addr.setJmxDomain(JMX_DOMAIN);
                ret.add(addr);
            }
        } else { // NO PROPERTIES FILE. FETCH BEANS
            if (trace)
                log.trace("Fetching group member hosts from local JMX and using default configured remote JMX port.");
            try {
                // FETCH PATTERN
                pattern = generateMBeanPattern(QueryType.CACHE_MANAGER_TYPE, JMX_DOMAIN, null);

                names = jmxHelper.getLocalMBeans(pattern, null);
                if (names == null) {
                    if (debug)
                        log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                    return new ArrayList<PhysicalAddress>();
                }

                Iterator<ObjectName> it = names.iterator();

                while (it.hasNext()) {
                    objName = it.next();

                    addresses = (String) jmxHelper.getLocalMBeanAttribute(objName, PHYSICAL_ADDRESSES);
                    stringAddrs.addAll(ParsingHelper.parsePhysicalAddresses(addresses));
                    for (String a : stringAddrs) {
                        addr = new PhysicalAddress();
                        addr.setHost(a);
                        addr.setPort(REMOTE_JMX_PORT);
                        /*
                         * Using actual JMX domain for locally deployed app will
                         * limit remote results to only the same domains.
                         * However, results could be different from results
                         * obtained using properties file.
                         */
                        addr.setJmxDomain(objName.getDomain());
                        if (trace)
                            log.trace("Got group member physical address with host: " + addr.getHost() + " and port: " + addr.getPort() + " and JMX domain: " + addr.getJmxDomain());
                    }
                    ret.add(addr);
                }
            } catch (Exception e) {
                if (trace) {
                    log.trace("Stack trace:");
                    e.printStackTrace();
                }
                log.error("Error fetching physical addresses for cache group members.");
                //throw new RuntimeException("Error getting physical addresses for cache group members.", e);
            }
        }

        return ret;
    }

    public List<State> getState(String cacheName) {
        List<State> results = new ArrayList<State>();
        State state = null;
        Boolean joinComplete = null;
        Boolean xferInProgress = null;
        Set<ObjectName> names = null;
        List<Object> args = new ArrayList<Object>();
        String pattern = null;

        // FETCH PATTERN
        if (StringUtils.isBlank(cacheName)) {
            pattern = generateMBeanPattern(QueryType.DSTM_TYPE, JMX_DOMAIN, null);
        } else {
            args.addAll(Arrays.asList(ParsingHelper.toArgs(cacheName)));
            pattern = generateMBeanPattern(QueryType.DSTM_WITH_CACHE_TYPE, JMX_DOMAIN, args);
        }

        // FETCH BEANS
        try {
            // Should get one or more matching mbeans back. Otherwise, print a
            // warning.
            names = jmxHelper.getLocalMBeans(pattern, null);
            if (names == null) {
                if (debug)
                    log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                return new ArrayList<State>();
            }

            // FETCH ATTRIBUTES AND CREATE RESULTS
            if (names.isEmpty()) {
                log.warn("Got empty set from query for DistributedStateTransferManager MBeans.");
            } else {
                if (debug)
                    log.debug("Getting state information from " + names.size() + " mbeans.");
                Iterator<ObjectName> it = names.iterator();
                ObjectName obj = null;
                while (it.hasNext()) {
                    obj = it.next();
                    if (trace)
                        log.trace("Processing mbean: " + obj.toString());
                    joinComplete = (Boolean) jmxHelper.getLocalMBeanAttribute(obj, JOIN_COMPLETE);
                    xferInProgress = (Boolean) jmxHelper.getLocalMBeanAttribute(obj, XFER_IN_PROGRESS);

                    state = new State();
                    state.setNodeName(getNodeName(obj.getDomain()));
                    state.setJmxDomain(obj.getDomain());
                    // Put all states in a list, making sure they don't end up indicating the opposite
                    // E.g. a state transfer in progress (true) indicates the node is synchronizing, but
                    // a join being complete (true) indicates the opposite, that the node is available.
                    // Hence, one or more should be negated so make the meaning the same.
                    Boolean[] states = { joinComplete, !xferInProgress };
                    state.setState(ParsingHelper.calculateState(Arrays.asList(states)));
                    state.setJoinComplete(joinComplete);
                    state.setStateTransferInProgress(xferInProgress);
                    state.setCacheName(obj.getKeyProperty(CACHE_NAME_KEY));
                    state.setSimpleCacheName(getSimpleCacheName(obj.getDomain(), ParsingHelper.removeQuotes(obj.getKeyProperty(CACHE_NAME_KEY))));
                    state.setNumOwners(getNumOwners(obj.getDomain(), ParsingHelper.removeQuotes(obj.getKeyProperty(CACHE_NAME_KEY))));
                    results.add(state);
                }
            }
        } catch (Exception e) {
            if (trace) {
                log.trace("Stack trace:");
                e.printStackTrace();
            }
            throw new RuntimeException("Error fetching state information for cache through JMX.", e);
        }

        return results;
    }

    public List<MemoryUsage> getMemoryUsage(String cacheName) {
        List<MemoryUsage> results = new ArrayList<MemoryUsage>();
        MemoryUsage memoryUsage = null;
        String memUsage = null;
        Long totalMemory = null;
        Set<ObjectName> names = null;
        List<Object> args = new ArrayList<Object>();
        List<MemoryUser> memUsers = new ArrayList<MemoryUser>();
        String pattern = null;

        // FETCH PATTERN
        if (StringUtils.isBlank(cacheName)) {
            pattern = generateMBeanPattern(QueryType.MEMORY_USAGE_TYPE, JMX_DOMAIN, null);
        } else {
            args.addAll(Arrays.asList(ParsingHelper.toArgs(cacheName)));
            pattern = generateMBeanPattern(QueryType.MEMORY_USAGE_WITH_CACHE_TYPE, JMX_DOMAIN, args);
        }

        // FETCH BEANS
        try {
            // Should get one or more matching mbeans back. Otherwise, print a
            // warning.
            names = jmxHelper.getLocalMBeans(pattern, null);
            if (names == null) {
                if (debug)
                    log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                return new ArrayList<MemoryUsage>();
            }

            // FETCH ATTRIBUTES AND CREATE RESULTS
            if (names.isEmpty()) {
                log.warn("Got empty set from query for MemoryUsage MBeans.");
            } else {
                if (debug)
                    log.debug("Getting totalMemory and memoryUsage attributes (memoryusage) from " + names.size() + " mbeans.");
                Iterator<ObjectName> it = names.iterator();
                ObjectName obj = null;
                while (it.hasNext()) {
                    obj = it.next();
                    if (trace)
                        log.trace("Processing mbean: " + obj.toString());
                    totalMemory = (Long) jmxHelper.getLocalMBeanAttribute(obj, TOTAL_MEMORY);
                    memUsage = (String) jmxHelper.getLocalMBeanAttribute(obj, MEMORY_USAGE);

                    memUsers = ParsingHelper.parseMemoryUsage(memUsage);

                    if (memUsers.isEmpty()) {
                        if (debug)
                            log.debug("Got empty memory users list, returning single result.");
                        memoryUsage = new MemoryUsage();
                        memoryUsage.setNodeName(getNodeName(obj.getDomain()));
                        memoryUsage.setJmxDomain(obj.getDomain());
                        memoryUsage.setCacheName(obj.getKeyProperty(CACHE_NAME_KEY));
                        memoryUsage.setSimpleCacheName(getSimpleCacheName(obj.getDomain(), ParsingHelper.removeQuotes(obj.getKeyProperty(CACHE_NAME_KEY))));
                        memoryUsage.setTotalMemory(totalMemory);
                        memoryUsage.setObjectType("");
                        memoryUsage.setObjectTypeMemory(0L);
                        results.add(memoryUsage);
                    } else {
                        for (MemoryUser m : memUsers) {
                            memoryUsage = new MemoryUsage();
                            memoryUsage.setNodeName(getNodeName(obj.getDomain()));
                            memoryUsage.setJmxDomain(obj.getDomain());
                            memoryUsage.setCacheName(obj.getKeyProperty(CACHE_NAME_KEY));
                            memoryUsage.setSimpleCacheName(getSimpleCacheName(obj.getDomain(), ParsingHelper.removeQuotes(obj.getKeyProperty(CACHE_NAME_KEY))));
                            memoryUsage.setTotalMemory(totalMemory);
                            memoryUsage.setObjectType(m.getObjectType());
                            memoryUsage.setObjectTypeMemory(m.getMemory());
                            if (trace)
                                log.trace("Adding memory user with object type: " + memoryUsage.getObjectType() + " and memory use: " + memoryUsage.getObjectTypeMemory());
                            results.add(memoryUsage);
                        }
                    }
                }
            }
        } catch (Exception e) {
            if (trace) {
                log.trace("Stack trace:");
                e.printStackTrace();
            }
            throw new RuntimeException("Error fetching memory usage information for cache through JMX.", e);
        }

        return results;
    }

    public List<RecordCount> getRecordCounts(String cacheName) {
        List<RecordCount> results = new ArrayList<RecordCount>();
        RecordCount recordCount = null;
        Integer numEntries = 0;
        Set<ObjectName> names = null;
        List<Object> args = new ArrayList<Object>();
        String pattern = null;

        // FETCH PATTERN
        if (StringUtils.isBlank(cacheName)) {
            pattern = generateMBeanPattern(QueryType.STATISTICS_TYPE, JMX_DOMAIN, null);
        } else {
            args.addAll(Arrays.asList(ParsingHelper.toArgs(cacheName)));
            pattern = generateMBeanPattern(QueryType.STATISTICS_WITH_CACHE_TYPE, JMX_DOMAIN, args);
        }

        // FETCH BEANS
        try {
            // Should get one or more matching mbeans back. Otherwise, print a
            // warning.
            names = jmxHelper.getLocalMBeans(pattern, null);
            if (names == null) {
                if (debug)
                    log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                return new ArrayList<RecordCount>();
            }

            // FETCH ATTRIBUTES AND CREATE RESULTS
            if (names.isEmpty()) {
                log.warn("Got empty set from query for Statistics MBeans.");
            } else {
                if (debug)
                    log.debug("Getting numberOfEntries attribute (record count) from " + names.size() + " mbeans.");
                Iterator<ObjectName> it = names.iterator();
                ObjectName obj = null;
                while (it.hasNext()) {
                    obj = it.next();
                    if (trace)
                        log.trace("Processing mbean: " + obj.toString());
                    numEntries = (Integer) jmxHelper.getLocalMBeanAttribute(obj, NUM_ENTRIES);

                    recordCount = new RecordCount();
                    recordCount.setNodeName(getNodeName(obj.getDomain()));
                    recordCount.setCount(numEntries);
                    recordCount.setCacheName(obj.getKeyProperty(CACHE_NAME_KEY));
                    recordCount.setSimpleCachNamee(getSimpleCacheName(obj.getDomain(), ParsingHelper.removeQuotes(obj.getKeyProperty(CACHE_NAME_KEY))));
                    recordCount.setJmxDomain(obj.getDomain());
                    recordCount.setNumOwners(getNumOwners(obj.getDomain(), ParsingHelper.removeQuotes(obj.getKeyProperty(CACHE_NAME_KEY))));
                    results.add(recordCount);
                }
            }
        } catch (Exception e) {
            if (trace) {
                log.trace("Stack trace:");
                e.printStackTrace();
            }
            log.error("Error fetching record counts for cache through JMX.");
            //throw new RuntimeException("Error fetching record counts for cache through JMX.", e);
        }

        return results;
    }

    public List<TransactionCount> getTransactionCounts(String cacheName) {
        List<TransactionCount> results = new ArrayList<TransactionCount>();
        TransactionCount tCount = null;
        Set<ObjectName> names = null;
        List<Object> args = new ArrayList<Object>();
        List<String> attributes = new ArrayList<String>();
        String pattern = null;
        Long time = 0L;

        // FETCH PATTERN
        if (StringUtils.isBlank(cacheName)) {
            pattern = generateMBeanPattern(QueryType.STATISTICS_TYPE, JMX_DOMAIN, null);
        } else {
            args.addAll(Arrays.asList(ParsingHelper.toArgs(cacheName)));
            pattern = generateMBeanPattern(QueryType.STATISTICS_WITH_CACHE_TYPE, JMX_DOMAIN, args);
        }

        // Iterate through every type of transaction (read/write/delete)
        for (TransactionType type : TransactionType.values()) {
            if (trace)
                log.trace("Processing transaction type " + type.name());
            // FETCH BEANS
            try {
                time = 0L;
                // Should get one or more matching mbeans back. Otherwise, print
                // a warning.
                names = jmxHelper.getLocalMBeans(pattern, null);
                if (names == null) {
                    if (debug)
                        log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                    return new ArrayList<TransactionCount>();
                }

                // FETCH ATTRIBUTES AND CREATE RESULTS
                if (names.isEmpty()) {
                    log.warn("Got empty set from query for Statistics MBeans.");
                } else {
                    if (debug)
                        log.debug("Getting numberOfEntries attribute (record count) from " + names.size() + " mbeans.");
                    Iterator<ObjectName> it = names.iterator();
                    ObjectName obj = null;
                    while (it.hasNext()) {
                        attributes.clear();
                        obj = it.next();
                        if (trace)
                            log.trace("Processing mbean: " + obj.toString());
                        tCount = new TransactionCount();
                        switch (type) {
                        case read:
                            attributes.addAll(Arrays.asList(READ_ATTRS));
                            break;
                        case write:
                            attributes.addAll(Arrays.asList(WRITE_ATTRS));
                            break;
                        case delete:
                            attributes.addAll(Arrays.asList(DELETE_ATTRS));
                            break;
                        }
                        tCount.setOperationType(type.name());
                        tCount.setCount(aggregateAttributes(obj, attributes));
                        tCount.setNodeName(getNodeName(obj.getDomain()));
                        tCount.setCacheName(obj.getKeyProperty(CACHE_NAME_KEY));
                        tCount.setSimpleCacheName(getSimpleCacheName(obj.getDomain(), ParsingHelper.removeQuotes(obj.getKeyProperty(CACHE_NAME_KEY))));
                        tCount.setJmxDomain(obj.getDomain());
                        time = (Long) jmxHelper.getLocalMBeanAttribute(obj, TIME);
                        tCount.setTime(time);
                        results.add(tCount);
                    }
                }
            } catch (Exception e) {
                if (trace) {
                    log.trace("Stack trace:");
                    e.printStackTrace();
                }
                log.error("Error fetching transaction countes for cache through JMX.");
                //throw new RuntimeException("Error fetching transaction counts for cache through JMX.", e);
            }
        }

        return results;
    }

    /*
     * Given a list of attribute names, and an ObjectName representing a JMX
     * bean, this method will query the JMX bean for each of the attributes and
     * add them all together. All attributes are assumed to be of type Long.
     */
    private long aggregateAttributes(ObjectName obj, List<String> attrs)
            throws Exception {
        long result = 0L;
        long tmp = 0L;
        if (trace)
            log.trace("Aggregating values for attributes " + attrs.toString());
        for (String attr : attrs) {
            tmp = (Long) jmxHelper.getLocalMBeanAttribute(obj, attr);
            if (trace)
                log.trace("Got value " + tmp + " for attribute " + attr);
            result += tmp;
            if (trace)
                log.trace("Aggregate value so far is " + result);
        }
        if (trace)
            log.trace("Returning aggregate result of " + result);
        return result;
    }

    public List<MessageCount> getIncomingMessageCount() {
        List<MessageCount> mCounts = new ArrayList<MessageCount>();
        MessageCount mCount = null;
        String pattern = null;
        Set<ObjectName> mbeans = null;
        ObjectName objName = null;
        Long result = 0L;

        // FETCH PATTERN
        pattern = generateMBeanPattern(QueryType.STATS_TYPE, JMX_DOMAIN, null);

        try {
            // FETCH BEANS
            mbeans = jmxHelper.getLocalMBeans(pattern, null);
            if (mbeans == null) {
                if (debug)
                    log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                return new ArrayList<MessageCount>();
            }

            // FETCH ATTRIBUTES AND CREATE RESULTS
            Iterator<ObjectName> it = mbeans.iterator();
            while (it.hasNext()) {
                objName = it.next();
                result = (Long) jmxHelper.getLocalMBeanAttribute(objName, INCOMING_MESSAGES);

                mCount = new MessageCount();
                mCount.setNodeName(getNodeName(objName.getDomain()));
                mCount.setCount(result);
                mCount.setJmxDomain(objName.getDomain());
                mCounts.add(mCount);
            }
        } catch (Exception e) {
            if (trace) {
                log.trace("Stack trace:");
                e.printStackTrace();
            }
            log.error("Error fetching incoming message count.");
            //throw new RuntimeException("Error fetching incoming message count.", e);
        }

        return mCounts;
    }

    public List<MessageCount> getOutgoingMessageCount() {
        List<MessageCount> mCounts = new ArrayList<MessageCount>();
        MessageCount mCount = null;
        String pattern = null;
        Set<ObjectName> mbeans = null;
        ObjectName objName = null;
        Long result = 0L;

        // FETCH PATTERN
        pattern = generateMBeanPattern(QueryType.STATS_TYPE, JMX_DOMAIN, null);

        try {
            // FETCH BEANS
            mbeans = jmxHelper.getLocalMBeans(pattern, null);
            if (mbeans == null) {
                if (debug)
                    log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                return new ArrayList<MessageCount>();
            }

            // FETCH ATTRIBUTES AND CREATE RESULTS
            Iterator<ObjectName> it = mbeans.iterator();
            while (it.hasNext()) {
                objName = it.next();
                result = (Long) jmxHelper.getLocalMBeanAttribute(objName, OUTGOING_MESSAGES);

                mCount = new MessageCount();
                mCount.setNodeName(getNodeName(objName.getDomain()));
                mCount.setCount(result);
                mCount.setJmxDomain(objName.getDomain());
                mCounts.add(mCount);
            }
        } catch (Exception e) {
            if (trace) {
                log.trace("Stack trace:");
                e.printStackTrace();
            }
            log.error("Error fetching outgoing message count.");
            //throw new RuntimeException("Error fetching outgoing message count.", e);
        }

        return mCounts;
    }

    public List<MessageCount> getIncomingByteCount() {
        List<MessageCount> mCounts = new ArrayList<MessageCount>();
        MessageCount mCount = null;
        String pattern = null;
        Set<ObjectName> mbeans = null;
        ObjectName objName = null;
        Long result = 0L;

        // FETCH PATTERN
        pattern = generateMBeanPattern(QueryType.STATS_TYPE, JMX_DOMAIN, null);

        try {
            // FETCH BEANS
            mbeans = jmxHelper.getLocalMBeans(pattern, null);
            if (mbeans == null) {
                if (debug)
                    log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                return new ArrayList<MessageCount>();
            }

            // FETCH ATTRIBUTES AND CREATE RESULTS
            Iterator<ObjectName> it = mbeans.iterator();
            while (it.hasNext()) {
                objName = it.next();
                result = (Long) jmxHelper.getLocalMBeanAttribute(objName, INCOMING_BYTES);

                mCount = new MessageCount();
                mCount.setNodeName(getNodeName(objName.getDomain()));
                mCount.setCount(result);
                mCount.setJmxDomain(objName.getDomain());
                mCounts.add(mCount);
            }
        } catch (Exception e) {
            if (trace) {
                log.trace("Stack trace:");
                e.printStackTrace();
            }
            log.error("Error fetching incoming byte count.");
            //throw new RuntimeException("Error fetching incoming byte count.", e);
        }

        return mCounts;
    }

    public List<GroupMessageCount> getIncomingGroupByteCount() {
        String pattern = null;
        RemoteJMXConnection rjc = null;
        List<GroupMessageCount> mCounts = new ArrayList<GroupMessageCount>();
        GroupMessageCount mCount = null;
        List<PhysicalAddress> addresses = null;
        Long result = 0L;
        ObjectName objName = null;

        // FETCH PATTERN
        pattern = generateMBeanPattern(QueryType.STATS_TYPE, JMX_DOMAIN, null);

        // FETCH GROUP MEMBER ADDRESSES
        addresses = getGroupPhysicalAddresses();

        for (PhysicalAddress addr : addresses) {
            try {
                rjc = jmxHelper.getRemoteMBeans(addr.getHost(), addr.getPort(), pattern, null);
                if (trace)
                    log.trace("Got remote Jmx connection.");
                if (rjc.getMbeans() == null) {
                    if (debug)
                        log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                    continue;
                }
                Iterator<ObjectName> it = rjc.getMbeans().iterator();
                while (it.hasNext()) {
                    objName = it.next();
                    result = (Long) jmxHelper.getRemoteMBeanAttribute(rjc.getConn(), objName, INCOMING_BYTES);

                    if (trace)
                        log.trace("Got incoming group byte count of " + result);

                    mCount = new GroupMessageCount();
                    mCount.setNodeName(getRemoteNodeName(addr, objName.getDomain()));
                    mCount.setCount(result);
                    mCount.setJmxDomain(objName.getDomain());
                    mCount.setGroupSize(getGroupSize(objName.getDomain()));
                    mCounts.add(mCount);
                }
            } catch (Exception e) {
                if (trace) {
                    log.trace("Stack trace:");
                    e.printStackTrace();
                }
                log.error("Error getting group incoming bytes.");
                //throw new RuntimeException("Error getting group incoming bytes.", e);
            } finally {
                if (rjc != null && rjc.getJmxConnector() != null) {
                    try {
                        rjc.getJmxConnector().close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        return mCounts;
    }

    /*
     * Gets number of outgoing bytes send by nodes on current physical server
     */
    public List<MessageCount> getOutgoingByteCount() {
        List<MessageCount> mCounts = new ArrayList<MessageCount>();
        MessageCount mCount = null;
        String pattern = null;
        Set<ObjectName> mbeans = null;
        ObjectName objName = null;
        Long result = 0L;

        // FETCH PATTERN
        pattern = generateMBeanPattern(QueryType.STATS_TYPE, JMX_DOMAIN, null);

        try {
            // FETCH BEANS
            mbeans = jmxHelper.getLocalMBeans(pattern, null);
            if (mbeans == null) {
                if (debug)
                    log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                return new ArrayList<MessageCount>();
            }

            // FETCH ATTRIBUTES AND CREATE RESULTS
            Iterator<ObjectName> it = mbeans.iterator();
            while (it.hasNext()) {
                objName = it.next();
                result = (Long) jmxHelper.getLocalMBeanAttribute(objName, OUTGOING_BYTES);

                mCount = new MessageCount();
                mCount.setNodeName(getNodeName(objName.getDomain()));
                mCount.setCount(result);
                mCount.setJmxDomain(objName.getDomain());
                mCounts.add(mCount);
            }
        } catch (Exception e) {
            if (trace) {
                log.trace("Stack trace:");
                e.printStackTrace();
            }
            log.error("Error fetching outgoing byte count.");
            //throw new RuntimeException("Error fetching outgoing byte count.", e);
        }

        return mCounts;
    }

    /*
     * Gets the number of outgoing bytes sent by current node, as well as all
     * nodes that are also members of the same JGroups cluster, and returns all
     * results in a list.
     */
    public List<GroupMessageCount> getOutgoingGroupByteCount() {
        String pattern = null;
        RemoteJMXConnection rjc = null;
        List<GroupMessageCount> mCounts = new ArrayList<GroupMessageCount>();
        GroupMessageCount mCount = null;
        List<PhysicalAddress> addresses = null;
        Long result = 0L;
        ObjectName objName = null;

        // FETCH PATTERN
        pattern = generateMBeanPattern(QueryType.STATS_TYPE, JMX_DOMAIN, null);

        // FETCH GROUP MEMBER ADDRESSES
        addresses = getGroupPhysicalAddresses();

        for (PhysicalAddress addr : addresses) {
            try {
                rjc = jmxHelper.getRemoteMBeans(addr.getHost(), addr.getPort(), pattern, null);
                if (trace)
                    log.trace("Got remote JMX connection.");
                if (rjc.getMbeans() == null) {
                    if (debug)
                        log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                    continue;
                }
                Iterator<ObjectName> it = rjc.getMbeans().iterator();
                while (it.hasNext()) {
                    objName = it.next();
                    result = (Long) jmxHelper.getRemoteMBeanAttribute(rjc.getConn(), objName, OUTGOING_BYTES);

                    if (trace)
                        log.trace("Got outgoing group byte count of " + result);

                    mCount = new GroupMessageCount();
                    mCount.setNodeName(getRemoteNodeName(addr, objName.getDomain()));
                    mCount.setCount(result);
                    mCount.setJmxDomain(objName.getDomain());
                    mCount.setGroupSize(getGroupSize(objName.getDomain()));
                    mCounts.add(mCount);
                }
            } catch (Exception e) {
                if (trace) {
                    log.trace("Stack trace:");
                    e.printStackTrace();
                }
                throw new RuntimeException("Error getting group outgoing bytes.", e);
            } finally {
                if (rjc != null && rjc.getJmxConnector() != null) {
                    try {
                        rjc.getJmxConnector().close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        return mCounts;
    }

    /*
     * Loads the JGroups name of this node/cluster member. In this case, the
     * JGroups node name is stored in Infinispan's CacheManager JMX bean, so it
     * is fetched from there, rather than from a JGroups JMX bean. The JGroups
     * name defaults to the hostname of the node, plus a random number. This
     * name is re-generated under certain circumstances, which may cause the
     * random number to be different.
     */
    public String getNodeName(String jmxDomain) {
        String pattern = null;
        String nodeName = null;
        Set<ObjectName> names = null;
        ObjectName obj = null;

        try {
            pattern = generateMBeanPattern(QueryType.CACHE_MANAGER_TYPE, jmxDomain, null);

            if (debug)
                log.debug("Got CacheManager pattern: '" + pattern + "'.");

            names = jmxHelper.getLocalMBeans(pattern, null);
            if (names == null) {
                if (debug)
                    log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                return new String("");
            }

            if (names.size() > 1) {
                log.warn("More than one CacheManager MBean was returned. Taking the first one.");
            }
            obj = names.iterator().next();
            nodeName = (String) jmxHelper.getLocalMBeanAttribute(obj, NODE_NAME);

            if (debug)
                log.debug("Got nodeName value of '" + nodeName + "'.");
        } catch (Exception e) {
            if (trace) {
                log.trace("Stack trace:");
                e.printStackTrace();
            }
            log.error("Error fetching node name.");
            //throw new RuntimeException("Error fetching node name.", e);
        }

        return nodeName;
    }

    public String getSimpleNodeName(String jmxDomain, String cacheName) {
        String pattern = null;
        String simpleCacheName = null;
        Set<ObjectName> names = null;
        ObjectName obj = null;

        try {
            pattern = generateMBeanPattern(QueryType.CACHE_WITH_CACHE_TYPE, jmxDomain, null);

            if (debug)
                log.debug("Got Cache pattern: '" + pattern + "'.");

            names = jmxHelper.getLocalMBeans(pattern, null);
            if (names == null) {
                if (debug)
                    log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                return new String("");
            }

            if (names.size() > 1) {
                log.warn("More than one Cache MBean was returned. Taking the first one.");
            }
            obj = names.iterator().next();
            simpleCacheName = (String) jmxHelper.getLocalMBeanAttribute(obj, SIMPLE_CACHE_NAME);

            if (debug)
                log.debug("Got simpleCacheName value of '" + simpleCacheName + "'.");
        } catch (Exception e) {
            if (trace) {
                log.trace("Stack trace:");
                e.printStackTrace();
            }
            log.error("Error fetching simple cache name.");
            //throw new RuntimeException("Error fetching simple cache name.", e);
        }

        return simpleCacheName;
    }

    public String getNumOwners(String jmxDomain, String cache) {
        String pattern = null;
        List<Object> args = new ArrayList<Object>();
        String numOwners = null;
        Set<ObjectName> names = null;
        ObjectName obj = null;

        try {
            args.addAll(Arrays.asList(ParsingHelper.toArgs(cache)));
            pattern = generateMBeanPattern(QueryType.CACHE_WITH_CACHE_TYPE, jmxDomain, args);

            if (debug)
                log.debug("Got Cache pattern: '" + pattern + "'.");

            names = jmxHelper.getLocalMBeans(pattern, null);
            if (names == null) {
                if (debug)
                    log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                return new String("");
            }

            if (names.size() > 1) {
                log.warn("More than one Cache MBean was unexpectedly returned. Taking the first one.");
            }
            obj = names.iterator().next();
            numOwners = (String) jmxHelper.getLocalMBeanAttribute(obj, NUM_OWNERS);

            if (debug)
                log.debug("Got numOwners value of '" + numOwners + "'.");
        } catch (Exception e) {
            if (trace) {
                log.trace("Stack trace:");
                e.printStackTrace();
            }
            log.debug("Error fetching number of owners.");
            //throw new RuntimeException("Error fetching number of owners.", e);
        }

        return numOwners;
    }

    public String getSimpleCacheName(String jmxDomain, String cache) {
        String pattern = null;
        List<Object> args = new ArrayList<Object>();
        String simpleCacheName = null;
        Set<ObjectName> names = null;
        ObjectName obj = null;

        try {
            args.addAll(Arrays.asList(ParsingHelper.toArgs(cache)));
            pattern = generateMBeanPattern(QueryType.CACHE_WITH_CACHE_TYPE, jmxDomain, args);

            if (debug)
                log.debug("Got Cache pattern: '" + pattern + "'.");

            names = jmxHelper.getLocalMBeans(pattern, null);
            if (names == null) {
                if (debug)
                    log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                return new String("");
            }

            if (names.size() > 1) {
                log.warn("More than one Cache MBean was unexpectedly returned. Taking the first one.");
            }
            obj = names.iterator().next();
            simpleCacheName = (String) jmxHelper.getLocalMBeanAttribute(obj, SIMPLE_CACHE_NAME);

            if (debug)
                log.debug("Got simpleCacheName value of '" + simpleCacheName + "'.");
        } catch (Exception e) {
            if (trace) {
                log.trace("Stack trace:");
                e.printStackTrace();
            }
            log.error("Error fetching simple cache name.");
            //throw new RuntimeException("Error fetching simple cache name.", e);
        }

        return simpleCacheName;
    }

    public Integer getGroupSize(String jmxDomain) {
        String pattern = null;
        Integer groupSize = null;
        Set<ObjectName> names = null;
        ObjectName obj = null;

        try {
            pattern = generateMBeanPattern(QueryType.CACHE_MANAGER_TYPE, jmxDomain, null);

            if (debug)
                log.debug("Got CacheManager pattern: '" + pattern + "'.");

            names = jmxHelper.getLocalMBeans(pattern, null);
            if (names == null) {
                if (debug)
                    log.debug("Received null result after querying MBean names with pattern '" + pattern + "'.");
                return new Integer(0);
            }

            if (names.size() > 1) {
                log.warn("More than one CacheManager MBean was unexpectedly returned. Taking the first one.");
            }
            obj = names.iterator().next();
            groupSize = (Integer) jmxHelper.getLocalMBeanAttribute(obj, GROUP_SIZE);

            if (debug)
                log.debug("Got groupSize value of '" + groupSize + "'.");
        } catch (Exception e) {
            if (trace) {
                log.trace("Stack trace:");
                e.printStackTrace();
            }
            log.error("Error fetching group size.");
            //throw new RuntimeException("Error fetching group size.", e);
        }

        return groupSize;
    }

    /*
     * Same as getNodeName, but makes a JMX call to a remote server to fetch the
     * information
     */
    public String getRemoteNodeName(PhysicalAddress addr, String jmxDomain) {
        String pattern = null;
        String nodeName = null;
        RemoteJMXConnection rjc = null;
        ObjectName obj = null;

        try {
            pattern = generateMBeanPattern(QueryType.CACHE_MANAGER_TYPE,
                    jmxDomain, null);

            if (debug)
                log.debug("Got CacheManager pattern: '" + pattern + "'.");
            rjc = jmxHelper.getRemoteMBeans(addr.getHost(), addr.getPort(), pattern, null);
            if (rjc.getMbeans().size() > 1) {
                log.warn("More than one CacheManager MBean was unexpectedly returned. Taking the first one.");
            }
            obj = rjc.getMbeans().iterator().next();
            nodeName = (String) jmxHelper.getRemoteMBeanAttribute(rjc.getConn(), obj, NODE_NAME);

            if (debug)
                log.debug("Got nodeName value of '" + nodeName + "'.");
        } catch (Exception e) {
            if (trace) {
                log.trace("Stack trace:");
                e.printStackTrace();
            }
            log.error("Error fetching node name.");
            //throw new RuntimeException("Error fetching node name.", e);
        }

        return nodeName;
    }

    /*
     * Infinispan's CacheMgmtInterceptor is exposed through JMX as an MBeans
     * called "Statistics". The full JMX identifier looks something like this:
     * org.infinispan:type=Cache,name="myCache(dist_sync)",manager=
     * "DefaultCacheManager",component=Statistics where "org.infinispan" is the
     * JMX domain, and the rest are identifying attributes. This method creates
     * an string which, in turn, can be used to instantiate an ObjectName with
     * enough information to query for the Statistics MBean corresponding to a
     * specific cache, or to query for Statistics MBeans for all running caches,
     * depending on whether a cache name was provided.
     */
    public String generateMBeanPattern(QueryType type, String jmx_domain,
            List<Object> args) {
        String pattern = null;

        if (args == null)
            args = new ArrayList<Object>();

        // Insert JMX Domain as first argument, since it's always the first
        // parameter in format string
        args.add(0, jmx_domain);

        if (debug)
            log.debug("Constructing statistics query with arguments: '" + args.toString() + "'.");

        pattern = type.getPattern(args);

        if (debug) {
            log.debug("Returning statistics query pattern: '" + pattern + "'.");
        }

        return pattern;
    }

}
