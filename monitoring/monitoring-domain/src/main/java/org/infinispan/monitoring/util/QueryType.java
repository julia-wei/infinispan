package org.infinispan.monitoring.util;

import java.util.List;

public enum QueryType {

    // The numParams value should include the JMX Domain
    STATISTICS_TYPE("STATISTICS", "%s:type=Cache,component=Statistics,*", 1),
    STATISTICS_WITH_CACHE_TYPE("STATISTICS_WITH_CACHE", "%s:type=Cache,name=\"%s\",component=Statistics,*", 2),
    MEMORY_USAGE_TYPE("MEMORY_USAGE", "%s:type=Cache,component=MemoryUsage,*", 1),
    MEMORY_USAGE_WITH_CACHE_TYPE("MEMORY_USAGE_WITH_CACHE", "%s:type=Cache,name=\"%s\",component=MemoryUsage,*", 2),
    DSTM_TYPE("DSTM", "%s:type=Cache,component=StateTransferManager,*", 1),
    DSTM_WITH_CACHE_TYPE("DSTM_WITH_CACHE", "%s:type=Cache,name=\"%s\",component=StateTransferManager,*", 2),
    CHANNEL_TYPE("CHANNEL", "%s:type=channel,*", 1),
    CHANNEL_WITH_CLUSTER_TYPE("CHANNEL_WITH_CLUSTER", "%s:type=channel,cluster=%s,*", 2),
    CACHE_MANAGER_TYPE("CACHE_MANAGER", "%s:type=CacheManager,component=CacheManager,*", 1),
    CACHE_TYPE("CACHE", "%s:type=Cache,component=Cache,*", 1),
    CACHE_WITH_CACHE_TYPE("CACHE_WITH_CACHE", "%s:type=Cache,name=\"%s\",component=Cache,*", 2),
    STATS_TYPE("JGROUPS_STATS", "%s:protocol=STATS,*", 1);

    String type;
    String pattern;
    int numParams;

    private QueryType(String type, String pattern, int numParams) {
        this.type = type;
        this.pattern = pattern;
        this.numParams = numParams;
    }

    public String getPattern(List<Object> args) {

        if (args.size() != numParams) {
            throw new RuntimeException("Error generating " + type + " pattern. Expected " + numParams + " arguments, including JMX Domain, but got " + args.size());
        }

        return String.format(pattern, args.toArray());
    }

}
