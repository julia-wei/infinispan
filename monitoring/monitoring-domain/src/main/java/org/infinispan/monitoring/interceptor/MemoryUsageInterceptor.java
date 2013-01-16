package org.infinispan.monitoring.interceptor;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.monitoring.interceptor.memory.MemoryUsageKeyEntry;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.libra.Libra;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

@MBean(objectName = "MemoryUsage", description = "Measures memory usage for cache, either through JBoss Libra Java Agent or by simple object counts")
public class MemoryUsageInterceptor extends JmxStatsCommandInterceptor {

    protected Map<String, AtomicLong> usageMap = new HashMap<String, AtomicLong>();
    protected Map<Object, MemoryUsageKeyEntry> sizeMap = new HashMap<Object, MemoryUsageKeyEntry>();
    protected AtomicLong totalSize = new AtomicLong(0);

    private Boolean useAgent = Boolean.TRUE;

    private static String METRIC_TYPE_OBJECT_SIZE = "Object Size";
    private static String METRIC_TYPE_OBJECT_COUNT = "Object Count";

    private static final Log log = LogFactory.getLog(MemoryUsageInterceptor.class);
    private static boolean debug = log.isDebugEnabled();
    private static boolean trace = log.isTraceEnabled();

    // Map.put(key,value) :: oldValue
    public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {

        Object retval = invokeNextInterceptor(ctx, command);
        if (trace)
            log.trace("PUTKEYVALUE value is '" + command.getValue() + "' and of type '" + command.getValue().getClass().getName() + "'.");
        if (command.isSuccessful()) {
            handleAddOrUpdate(command.getKey(), command.getValue());
        }
        return retval;
    }

    @Override
    public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
        Map<Object, Object> data = command.getMap();
        Object retval = invokeNextInterceptor(ctx, command);
        if (data != null && !data.isEmpty() && command.isSuccessful()) {
            for (Entry<Object, Object> entry : data.entrySet()) {
                handleAddOrUpdate(entry.getKey(), entry.getValue());
            }
        }
        return retval;
    }

    @Override
    public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
        Object retval = invokeNextInterceptor(ctx, command);
        if (command.isSuccessful()) {
            handleRemove(command.getKey());
        }
        return retval;
    }

    @Override
    public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
        Object retval = invokeNextInterceptor(ctx, command);
        if (command.isSuccessful() && retval != null) {
            handleRemove(command.getKey());
        }
        return retval;
    }

    @Override
    public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
        Object retval = invokeNextInterceptor(ctx, command);
        if (command.isSuccessful()) {
            reset();
        }
        return retval;
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
                toggleMeasurementType();
                log.infof(
                        "Unable to get object size using Libra, so reverting to object count. To track object size, make sure JBoss Libra is on the system classpath, or the server is started with -javaagent:<path-to-libra>. Error was: %s",
                        e.toString());
                if (debug)
                    e.printStackTrace();
            }
        }

        if (!useAgent) {
            size = 1L;
        }

        if (trace)
            log.tracef("Calculated size for object '%s' is '%d'", obj.toString(), size);

        return size;
    }

    private void handleAddOrUpdate(Object key, Object value) {
        Long size = 0L;
        Long oldSize = 0L;
        MemoryUsageKeyEntry keyEntry = null;
        String objType = value.getClass().getName();

        size = getMemoryUsage(value);

        if (trace)
            log.tracef("Handling add or update for value with key '%s', size '%d' and type '%s'.", key.toString(), size, objType);

        // Initialize map entry for object type, if necessary
        if (!usageMap.containsKey(objType)) {
            if (trace)
                log.tracef("Initializing map entry for object type '%s'.", objType);
            usageMap.put(objType, new AtomicLong(0));
        }

        // Now that usageMap entry should have been initialized, print trace info.
        if (trace)
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
            if (trace)
                log.tracef("Updating memory usage for key '%s'. Subtracting '%d' from size.", key.toString(), oldSize);
        } else { // store size value for new cache entry
            if (trace)
                log.tracef("Tracking new cache entry with key '%s', type '%s' and size '%d'", key.toString(), objType, size);
            keyEntry = new MemoryUsageKeyEntry(key, objType, size);
            sizeMap.put(key, keyEntry);
        }

        // Add new size to running totals
        usageMap.get(objType).getAndAdd(size);
        totalSize.getAndAdd(size);

        if (trace)
            log.tracef("Total size AFTER object add or update is '%d' and size for type '%s' is now '%d'.", totalSize.get(), objType, usageMap.get(objType).get());
    }

    private void handleRemove(Object key) {
        MemoryUsageKeyEntry keyEntry = null;
        String objType;
        Long size = 0L;

        if (trace)
            log.tracef("In handleRemove for key '%s'", key.toString());

        if (sizeMap.containsKey(key)) {
            if (trace)
                log.tracef("Before remove, sizeMap has '%d' entries.", sizeMap.size());

            // stop tracking removed entry
            keyEntry = sizeMap.get(key);
            objType = keyEntry.getType();
            size = keyEntry.getSize().get();
            sizeMap.remove(key);

            if (trace)
                log.tracef("After remove, sizeMap has '%d' entries.", sizeMap.size());

            if (trace)
                log.tracef("Handling remove for object with key '%s', type '%s' and size '%d'", key.toString(), objType, size);
            if (trace)
                log.tracef("Total size BEFORE object remove is '%d' and size for type '%s' is now '%d'.", totalSize.get(), objType, usageMap.get(objType).get());
        } else {
            if (debug)
                log.debugf("Was asked to process removal of entry with key '%s', which isn't being tracked. Doing nothing.", key.toString());
            return;
        }

        if (usageMap.containsKey(objType)) {
            // subtract size of removed entry from running totals
            usageMap.get(objType).getAndAdd(0 - size);
            totalSize.getAndAdd(0 - size);
        } else {
            if (debug)
                log.debugf("Was asked to process removal of entry with key '%s' and of type '%s', but that type isn't being tracked. Total for type will not be decremented.", key.toString(), objType);
            return;
        }

        if (trace)
            log.tracef("Total size AFTER object remove is '%d' and size for type '%s' is now '%d'.", totalSize.get(), objType, usageMap.get(objType).get());
    }

    public String getMemoryUsageAsString() {
        return usageMap.toString();
    }

}
