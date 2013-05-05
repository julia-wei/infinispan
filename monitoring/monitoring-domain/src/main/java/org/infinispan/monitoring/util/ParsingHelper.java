package org.infinispan.monitoring.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ParsingHelper {
    private static Log log = LogFactory.getLog(ParsingHelper.class);
    private static boolean debug = log.isDebugEnabled();
    private static boolean trace = log.isTraceEnabled();

    /*
     * Expects a string in the format returned by ISPN's CacheManager MBean for
     * the physicalAddresses attribute (I.e. [ ip:port, ip:port, ... ] and
     * parses said string into a list of strings, where each string is the ip
     * address from the provided list, with the brackets, colon and port
     * stripped.
     */
    public static List<String> parsePhysicalAddresses(String addys) {
        List<String> results = new ArrayList<String>();
        List<String> temp = new ArrayList<String>();
        String tmp = new String(addys);

        // Remove square brackets from array's "toString"
        tmp = tmp.replaceAll("\\[|\\]", "");
        // Split content of array, which will yield a list in format "host:port"
        temp.addAll(Arrays.asList(tmp.split(", ")));
        //Remove ":port" from end of each string.
        for (String str : temp) {
            str = new String(Arrays.asList(str.split(":")).get(0));
            if (debug)
                log.debug("Got host: " + str);
            results.add(str);
        }

        return results;
    }

    public static List<MemoryUser> parseMemoryUsage(String memoryUsage) {
        List<MemoryUser> memUsers = new ArrayList<MemoryUser>();
        List<String> stuff = new ArrayList<String>();
        MemoryUser user = null;
        String str = new String(memoryUsage);
        
        if("{}".equals(memoryUsage)) {
            if(debug)
                log.debug("Memory usage string is empty, returning empty list. Usage string: "+memoryUsage);
            return memUsers;
        }

        if (debug)
            log.debug("Parsing memory usage string: " + str);
        str = str.replaceAll("\\{|\\}", "");
        if (trace)
            log.trace("After replaceAll{}: " + str);
        stuff.addAll(Arrays.asList(str.split(", ")));
        for (String s : stuff) {
            if (trace)
                log.trace("Got memory user: " + s);
            user = new MemoryUser();
            user.setObjectType(Arrays.asList(s.split("=")).get(0));
            user.setMemory(Long.decode(Arrays.asList(s.split("=")).get(1)));
            memUsers.add(user);
            if (trace)
                log.trace("Parsed a memory usage entry. Type: " + user.getObjectType() + " size: " + user.getMemory());
        }
        
        if(trace) {
            log.trace("List of parsed entries:");
            log.trace(memUsers.toString());
        }
        
        if(debug)
            log.debug("Done parsing memory usage string");

        return memUsers;
    }

    public static String removeQuotes(String str) {
        String tmp = new String(str);

        tmp = tmp.replaceAll("\"", "");

        return tmp;
    }

    public static Object[] toArgs(String arg) {
        Object[] ret = null;

        if (trace)
            log.trace("Converting '" + arg + "' to args.");

        if (arg == null || "".equals(arg)) {
            ret = new Object[0];
        } else {
            ret = new Object[] { arg };
        }

        return ret;
    }

    public static String calculateState(List<Boolean> states) {

        if (states.contains(Boolean.FALSE)) {
            return "Synchronizing";
        } else {
            return "Available";
        }
    }
}
