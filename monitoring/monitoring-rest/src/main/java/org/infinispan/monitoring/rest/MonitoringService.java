package org.infinispan.monitoring.rest;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.infinispan.monitoring.util.JmxQueryHelper;

import org.infinispan.monitoring.domain.GroupMessageCount;
import org.infinispan.monitoring.domain.MemoryUsage;
import org.infinispan.monitoring.domain.MessageCount;
import org.infinispan.monitoring.domain.RecordCount;
import org.infinispan.monitoring.domain.State;
import org.infinispan.monitoring.domain.TransactionCount;

/**
 * @author dvb
 */
@Path("/monitoring")
public class MonitoringService {
    private static Log log = LogFactory.getLog(MonitoringService.class);
    //private static boolean debug = log.isDebugEnabled();
    private static boolean trace = log.isTraceEnabled();

    private static JmxQueryHelper jmxQueryHelper = new JmxQueryHelper();

    @GET()
    @Path("/state")
    @Produces(MediaType.APPLICATION_JSON)
    public List<State> getState() {
        if (trace)
            log.trace("In REST service. Fetching state for all caches.");
        return getState(null);
    }

    @GET()
    @Path("/state/{cacheName}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<State> getState(@PathParam("cacheName") String cacheName) {
        if (trace)
            log.trace("In REST service. Fetching state with cache name of '" + cacheName + "'.");
        return jmxQueryHelper.getState(cacheName);
    }
    
    @GET
    @Path("/memoryusage")
    @Produces(MediaType.APPLICATION_JSON)
    public List<MemoryUsage> getMemoryUsage() {
        if(trace)
            log.trace("In REST service. Fetching memory usage for all caches.");
        return getMemoryUsage(null);
    }
    
    @GET
    @Path("/memoryusage/{cacheName}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<MemoryUsage> getMemoryUsage(@PathParam("cacheName") String cacheName) {
        if(trace)
            log.trace("In REST service. Fetching memory usage for all caches.");
        return jmxQueryHelper.getMemoryUsage(cacheName);
    }

    @GET()
    @Path("/recordcount")
    @Produces(MediaType.APPLICATION_JSON)
    public List<RecordCount> getRecordCount() {
        if (trace)
            log.trace("In REST service. Fetching record count for all caches.");
        return getRecordCount(null);
    }

    @GET()
    @Path("/recordcount/{cacheName}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<RecordCount> getRecordCount(@PathParam("cacheName") String cacheName) {
        if (trace)
            log.trace("In REST service. Fetching record count with cache name of '"
                    + cacheName + "'.");
        return jmxQueryHelper.getRecordCounts(cacheName);
    }

    @GET()
    @Path("/incomingmessages")
    @Produces(MediaType.APPLICATION_JSON)
    public List<MessageCount> getIncomingMessages() {
        if (trace)
            log.trace("In REST service. Fetching incoming message count.");
        return jmxQueryHelper.getIncomingMessageCount();
    }

    @GET()
    @Path("/outgoingmessages")
    @Produces(MediaType.APPLICATION_JSON)
    public List<MessageCount> getOutgoingMessages() {
        if (trace)
            log.trace("In REST service. Fetching outgoing message count.");
        return jmxQueryHelper.getOutgoingMessageCount();
    }

    @GET()
    @Path("/incomingbytes")
    @Produces(MediaType.APPLICATION_JSON)
    public List<MessageCount> getIncomingBytes() {
        if (trace)
            log.trace("In REST service. Fetching incoming byte count.");
        return jmxQueryHelper.getIncomingByteCount();
    }

    @GET()
    @Path("/incominggroupbytes")
    @Produces(MediaType.APPLICATION_JSON)
    public List<GroupMessageCount> getIncomingGroupBytes() {
        if (trace)
            log.trace("In REST service. Fetching incoming byte count.");
        return jmxQueryHelper.getIncomingGroupByteCount();
    }

    @GET()
    @Path("/outgoingbytes")
    @Produces(MediaType.APPLICATION_JSON)
    public List<MessageCount> getOutgoingBytes() {
        if (trace)
            log.trace("In REST service. Fetching outgoing byte count.");
        return jmxQueryHelper.getOutgoingByteCount();
    }

    @GET()
    @Path("/outgoinggroupbytes")
    @Produces(MediaType.APPLICATION_JSON)
    public List<GroupMessageCount> getOutgoingGroupBytes() {
        if (trace)
            log.trace("In REST service. Fetching outgoing byte count.");
        return jmxQueryHelper.getOutgoingGroupByteCount();
    }
    
    @GET()
    @Path("/groupsize/{jmxDomain}")
    @Produces(MediaType.APPLICATION_JSON)
    public Integer getGroupSize(@PathParam("jmxDomain") String jmxDomain) {
        if(trace)
            log.trace("In REST service. Fetching group size.");
        return jmxQueryHelper.getGroupSize(jmxDomain);
    }
    
    @GET()
    @Path("/numOwners/{jmxDomain}/{cacheName}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getNumOwners(@PathParam("jmxDomain") String jmxDomain, @PathParam("cacheName") String cacheName) {
        if(trace)
            log.trace("In REST service. Fetching number of owners.");
        return jmxQueryHelper.getNumOwners(jmxDomain, cacheName);
    }

    @GET()
    @Path("/transactions")
    @Produces(MediaType.APPLICATION_JSON)
    public List<TransactionCount> getTransactionCounts() {
        if (trace)
            log.trace("In REST service. Fetching transaction counts for all caches.");
        return getTransactionCounts(null);
    }

    @GET()
    @Path("/transactions/{cacheName}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<TransactionCount> getTransactionCounts(@PathParam("cacheName") String cacheName) {
        if (trace)
            log.trace("In REST service. Fetching transaction counts.");
        return jmxQueryHelper.getTransactionCounts(cacheName);
    }
}
