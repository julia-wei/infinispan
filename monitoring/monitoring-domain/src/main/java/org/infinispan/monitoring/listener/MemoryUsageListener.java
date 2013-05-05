package org.infinispan.monitoring.listener;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.monitoring.listener.memory.MemoryUsageKeyEntry;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryInvalidatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.libra.Libra;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

@Listener(sync = false)
@MBean(objectName = "MemoryUsage", description = "Measures memory usage for cache, either through JBoss Libra Java Agent or by simple object counts")
public class MemoryUsageListener extends JmxStatsCommandInterceptor {

    protected Map<Serializable, AtomicLong> usageMap = new ConcurrentHashMap<Serializable, AtomicLong>();
    protected Map<Object, MemoryUsageKeyEntry> sizeMap = new ConcurrentHashMap<Object, MemoryUsageKeyEntry>();
    protected AtomicLong totalSize = new AtomicLong(0);

    private Boolean useAgent = Boolean.TRUE;

    private static String METRIC_TYPE_OBJECT_SIZE = "Object Size";
    private static String METRIC_TYPE_OBJECT_COUNT = "Object Count";

    private static final Log log = LogFactory.getLog(MemoryUsageListener.class);

    @SuppressWarnings("rawtypes")
    @CacheEntryRemoved
    public void entryRemoved(CacheEntryRemovedEvent event) throws Exception {
        if (!event.isPre()) {
            log.tracef("Received CacheEntryRemovedEvent: %s", event); 
            handleRemove(event.getKey());
        }        
    }
    
    @SuppressWarnings("rawtypes")
    @CacheEntryModified
    public void entryModified(CacheEntryModifiedEvent event) throws Exception {
        if (!event.isPre()) {
            log.tracef("Received CacheEntryModifiedEvent: %s", event);
            handleAddOrUpdate(event.getKey(), event.getValue());
        }
    }
    
    @SuppressWarnings("rawtypes")
    @CacheEntryActivated
    public void entryActivated(CacheEntryActivatedEvent event) throws Exception {        
        log.tracef("Received CacheEntryActivatedEvent: %s", event);
        handleAddOrUpdate(event.getKey(), event.getValue());        
    }
    
    @SuppressWarnings("rawtypes")
    @CacheEntryPassivated
    public void entryPassivated(CacheEntryPassivatedEvent event) throws Exception {        
        log.tracef("entryPassivated event=" + event); 
        //handleAddOrUpdate(event.getKey());        
    }
    
    @SuppressWarnings("rawtypes")
    @CacheEntriesEvicted
    public void entriesEvicted(CacheEntriesEvictedEvent event) throws Exception {        
        log.tracef("Received CacheEntriesEvictedEvent: %s", event);
        Map entries = event.getEntries();            
        if (entries != null) {
            Set keySet = entries.keySet();                
            for (Object key:keySet) {
                handleRemove(key);
            }                
        } 
    }
    

    @SuppressWarnings("rawtypes")
    @CacheEntryInvalidated
    public void entryInvalidated(CacheEntryInvalidatedEvent event) throws Exception {
        //CacheEntryInvalidatedEvent always has pre=true
        log.tracef("Received CacheEntryInvalidatedEvent: %s", event); 
        handleRemove(event.getKey());        
    }
    
    @SuppressWarnings("rawtypes")
    @DataRehashed
    public void dataRehashed(DataRehashedEvent event) throws Exception {        
        log.tracef("Received DataRehashedEvent: %s", event); 
    }
    
    @SuppressWarnings("rawtypes")
    @TopologyChanged
    public void topologyChanged(TopologyChangedEvent event) throws Exception {        
        log.tracef("Received TopologyChangedEvent: %s", event);         
    }
      
    

    @ManagedAttribute(description = "string representation of memory usage, or object count, per object type")
    @Metric(displayName = "Memory use, or object count, per object type", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public String getMemoryUsage() {
        return getMemoryUsageAsString();
    }

    @ManagedAttribute(description = "total memory used, or object count, across all object types")
    @Metric(displayName = "Memory use, or object count, for all object types", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public Long getTotalMemory() {
        return totalSize.get();
    }

    @ManagedAttribute(description = "whether object sizes or object counts are being tracked.")
    @Metric(displayName = "Type of metric tracking being used.")
    public String getTrackingType() {
        return useAgent ? METRIC_TYPE_OBJECT_SIZE : METRIC_TYPE_OBJECT_COUNT;
    }

    @Override
    @ManagedOperation(description = "Resets statistics gathered by this component")
    @Operation(displayName = "Reset Statistics (Statistics)")
    public void resetStatistics() {
        this.reset();
    }

    @ManagedOperation(description = "Toggles between gathering actual memory size and storing only a simple object count.")
    @Operation(displayName = "Toggle between object size and object count measurement types.")
    public void toggleMeasurementType() {
        useAgent = !useAgent;
        reset();
    }

    public void reset() {
        usageMap.clear();
        sizeMap.clear();
        totalSize.set(0);
    }

    private Long getMemoryUsage(Object obj) {
        Long size = 0L;

        if (useAgent) {
            try {
                size = Libra.getDeepObjectSize(obj);
            } catch (Throwable e) {
                //toggleMeasurementType(); it is not thread safe
                useAgent = false;
                reset();
                
                log.infof(
                        "Unable to get object size using Libra, so reverting to object count. To track object size, make sure JBoss Libra is on the system classpath, or the server is started with -javaagent:<path-to-libra>. Error was: %s",
                        e.getClass());
                log.error("libra throwable", e);
                if (log.isDebugEnabled())
                    e.printStackTrace();
            }
        }

        if (!useAgent) {
            size = 1L;
        }

        if (log.isTraceEnabled())
            log.tracef("Calculated size for object '%s' is '%d'", obj.toString(), size);

        return size;
    }

    private void handleAddOrUpdate(Object key, Object value) {
        Long size = 0L;
        Long oldSize = 0L;
        MemoryUsageKeyEntry keyEntry = null;
        String objType = value.getClass().getName();

        size = getMemoryUsage(value);

        if (log.isTraceEnabled())
            log.tracef("Handling add or update for value with key '%s', size '%d' and type '%s'.", key.toString(), size, objType);

        // Initialize map entry for object type, if necessary
        if (!usageMap.containsKey(objType)) {
            if (log.isTraceEnabled())
                log.tracef("Initializing map entry for object type '%s'.", objType);
            usageMap.put(objType, new AtomicLong(0));
        }

        // Now that usageMap entry should have been initialized, print trace info.
        if (log.isTraceEnabled())
            log.tracef("Total size BEFORE object add or update is '%d' and size for type '%s' is now '%d'.", totalSize.get(), objType, usageMap.get(objType).get());

        // If value is being updated for existing key, previous value should be subtracted
        if (sizeMap.containsKey(key)) {
            keyEntry = sizeMap.get(key);
            oldSize = keyEntry.getSize().get();
            // Update stored size value for key
            keyEntry.getSize().set(size);
            // Subtract key's previous stored size value from running totals
            usageMap.get(objType).getAndAdd(0 - oldSize);
            totalSize.getAndAdd(0 - oldSize);
            if (log.isTraceEnabled())
                log.tracef("Updating memory usage for key '%s'. Subtracting '%d' from size.", key.toString(), oldSize);
        } else { // store size value for new cache entry
            if (log.isTraceEnabled())
                log.tracef("Tracking new cache entry with key '%s', type '%s' and size '%d'", key.toString(), objType, size);
            keyEntry = new MemoryUsageKeyEntry(key, objType, size);
            sizeMap.put(key, keyEntry);
        }

        // Add new size to running totals
        usageMap.get(objType).getAndAdd(size);
        totalSize.getAndAdd(size);

        if (log.isTraceEnabled())
            log.tracef("Total size AFTER object add or update is '%d' and size for type '%s' is now '%d'.", totalSize.get(), objType, usageMap.get(objType).get());
            log.tracef("sizeMap size= '%s'", sizeMap.size());
    }

    private void handleRemove(Object key) {
        MemoryUsageKeyEntry keyEntry = null;
        String objType;
        Long size = 0L;

        if (log.isTraceEnabled())
            log.tracef("In handleRemove for key '%s'", key.toString());

        if (sizeMap.containsKey(key)) {
            if (log.isTraceEnabled())
                log.tracef("Before remove, sizeMap has '%d' entries.", sizeMap.size());

            // stop tracking removed entry
            keyEntry = sizeMap.get(key);
            objType = keyEntry.getType();
            size = keyEntry.getSize().get();
            sizeMap.remove(key);

            if (log.isTraceEnabled())
                log.tracef("After remove, sizeMap has '%d' entries.", sizeMap.size());

            if (log.isTraceEnabled())
                log.tracef("Handling remove for object with key '%s', type '%s' and size '%d'", key.toString(), objType, size);
            if (log.isTraceEnabled())
                log.tracef("Total size BEFORE object remove is '%d' and size for type '%s' is now '%d'.", totalSize.get(), objType, usageMap.get(objType).get());
        } else {
            if (log.isDebugEnabled())
                log.debugf("Was asked to process removal of entry with key '%s', which isn't being tracked. Doing nothing.", key.toString());
            return;
        }

        if (usageMap.containsKey(objType)) {
            // subtract size of removed entry from running totals
            usageMap.get(objType).getAndAdd(0 - size);
            totalSize.getAndAdd(0 - size);
        } else {
            if (log.isDebugEnabled())
                log.debugf("Was asked to process removal of entry with key '%s' and of type '%s', but that type isn't being tracked. Total for type will not be decremented.", key.toString(), objType);
            return;
        }

        if (log.isTraceEnabled())
            log.tracef("Total size AFTER object remove is '%d' and size for type '%s' is now '%d'.", totalSize.get(), objType, usageMap.get(objType).get());
    }

    public String getMemoryUsageAsString() {
        return usageMap.toString();
    }

}
