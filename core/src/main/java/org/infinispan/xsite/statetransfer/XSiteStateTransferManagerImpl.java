/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.xsite.statetransfer;

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;


/**
 *
 */
public class XSiteStateTransferManagerImpl implements XSiteStateTransferManager {

    private static Log log = LogFactory.getLog(XSiteStateTransferManagerImpl.class);
    private static final boolean trace = log.isTraceEnabled();
    private Transport transport;
    private ExecutorService asyncTransportExecutor;
    private GlobalComponentRegistry gcr;
    private LocalTopologyManager localTopologyManager;
    //TODO not sure if the configuration can be injected here
    private Configuration configuration;
    private BackupConfiguration bc;
    private boolean xsiteTransferRunning;
    private DefaultCacheManager defaultCacheManager;
    private GlobalConfiguration globalConfiguration;
    private String sourceSiteName;
    private Map<SiteCachePair, Future<Map<Address, Response>>> xSiteTransferBySiteFutureTasks = new ConcurrentHashMap<SiteCachePair, Future<Map<Address, Response>>>();
    private Map<SiteCachePair, Future<Object>> xSiteTransferBySiteLocalTasks = new ConcurrentHashMap<SiteCachePair, Future<Object>>();


    @Inject
    public void inject(Transport transport,
                       @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor,
                       GlobalComponentRegistry gcr, Configuration config, DefaultCacheManager defaultCacheManager, GlobalConfiguration globalConfiguration) {
        this.transport = transport;
        this.asyncTransportExecutor = asyncTransportExecutor;
        this.gcr = gcr;
        this.configuration = config;
        this.defaultCacheManager = defaultCacheManager;
        this.globalConfiguration = globalConfiguration;
        this.sourceSiteName = globalConfiguration.sites().localSite();

    }

    @Override
    public Set<XSiteStateTransferResponseInfo> pushState(String destinationSiteName) throws Exception {
        Set<XSiteStateTransferResponseInfo> xSiteStateTransferResponseInfos = new HashSet<XSiteStateTransferResponseInfo>();
        Set<String> cacheNames = defaultCacheManager.getCacheNames();
        for (String cacheName : cacheNames) {
            Set<XSiteStateTransferResponseInfo> responses = pushState(destinationSiteName, cacheName);
            if (responses != null) {
                xSiteStateTransferResponseInfos.addAll(responses);
            }
        }
        return xSiteStateTransferResponseInfos;
    }

    @Override
    public void cancelStateTransfer(String destinationSiteName, String cacheName)throws Exception {
        SiteCachePair siteCachePair = new SiteCachePair(destinationSiteName, cacheName);
        Future<Map<Address, Response>> remoteFuture = xSiteTransferBySiteFutureTasks.get(siteCachePair);
        Future<Object> localFuture = xSiteTransferBySiteLocalTasks.get(siteCachePair);
        if (remoteFuture != null) {
            remoteFuture.cancel(true);
        }
        if (localFuture != null) {
            localFuture.cancel(true);
        }
        Address address = transport.getAddress();
        XSiteStateRequestCommand xsiteStateRequestCommand = buildCommand(destinationSiteName, cacheName, address, XSiteStateRequestCommand.Type.START_XSITE_STATE_CANCEL);
         Map<Address, Object> results = executeOnClusterSync(xsiteStateRequestCommand, destinationSiteName, cacheName, bc.replicationTimeout(), false);
    }

    @Override
    public Set<XSiteStateTransferResponseInfo> pushState(String destinationSiteName, String cacheName) throws Exception {
        bc = getBackupConfigurationForSite(destinationSiteName);
        if (bc == null) {
            if (trace) {
                log.tracef("The current cache %s does not have any backup for the given site %s", cacheName, destinationSiteName);
                throw new Exception("The site name you specified is not one of the backup sites for the current site");
            }
        }
        xsiteTransferRunning = true;
        Address address = transport.getAddress();
        XSiteStateRequestCommand xsiteStateRequestCommand = buildCommand(destinationSiteName, cacheName, address, XSiteStateRequestCommand.Type.START_XSITE_STATE_TRANSFER);
        Set<XSiteStateTransferResponseInfo> responses = null;

        try {
            Map<Address, Object> results = executeOnClusterSync(xsiteStateRequestCommand, destinationSiteName, cacheName, bc.replicationTimeout(), false);
            if (results != null) {
                responses = buildResponseInfo(results, destinationSiteName, cacheName);
            }
            //TODO return object to be determined
            //TODO Exception handling to be determined
        } catch (Exception e) {
            e.printStackTrace();
        }
        removeCompletedTask(destinationSiteName, cacheName);
        return responses;

    }

    private void removeCompletedTask(String destinationSiteName, String cacheName) {
        SiteCachePair siteCachePair = new SiteCachePair(cacheName, destinationSiteName);
        xSiteTransferBySiteFutureTasks.remove(siteCachePair);
        xSiteTransferBySiteLocalTasks.remove(siteCachePair);
    }


    private Set<XSiteStateTransferResponseInfo> buildResponseInfo(Map<Address, Object> results, String siteName, String cacheName) {
        Set<XSiteStateTransferResponseInfo> xSiteStateTransferResponseInfos = new HashSet<XSiteStateTransferResponseInfo>();
        for (Map.Entry<Address, Object> entry : results.entrySet()) {
            XSiteStateTransferResponseInfo xSiteStateTransferResponseInfo = new XSiteStateTransferResponseInfo(siteName, entry.getKey(), (Set<Object>) entry.getValue(), cacheName);
            xSiteStateTransferResponseInfos.add(xSiteStateTransferResponseInfo);
        }
        return xSiteStateTransferResponseInfos;
    }

    private XSiteStateRequestCommand buildCommand(String siteName, String cacheName, Address address, XSiteStateRequestCommand.Type type) {
        XSiteStateRequestCommand xSiteStateRequestCommand = new XSiteStateRequestCommand(siteName, sourceSiteName, cacheName, address, type);
        return xSiteStateRequestCommand;
    }

    private BackupConfiguration getBackupConfigurationForSite(String destinationSiteName) {

        for (BackupConfiguration bc : configuration.sites().inUseBackups()) {
            if (bc.site().equals(destinationSiteName)) {
                return bc;
            }
        }
        return null;
    }

    private Map<Address, Object> executeOnClusterSync(final ReplicableCommand command, String destinationSiteName, String cacheName, final long timeout, boolean isCancelled)
            throws Exception {
        // first invoke remotely
        Future<Map<Address, Response>> remoteFuture = asyncTransportExecutor.submit(new Callable<Map<Address, Response>>() {
            @Override
            public Map<Address, Response> call() throws Exception {
                return transport.invokeRemotely(null, command,
                        ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, timeout, true, null);
            }
        });

        // now invoke the command on the local node
        Future<Object> localFuture = asyncTransportExecutor.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                gcr.wireDependencies(command);
                try {
                    return command.perform(null);
                } catch (Throwable t) {
                    throw new Exception(t);
                }
            }
        });
        if (!isCancelled) {
            SiteCachePair siteCachePair = new SiteCachePair(cacheName, destinationSiteName);
            xSiteTransferBySiteFutureTasks.put(siteCachePair, remoteFuture);
            xSiteTransferBySiteLocalTasks.put(siteCachePair, localFuture);
        }

        // wait for the remote commands to finish
        Map<Address, Response> responseMap = remoteFuture.get(timeout, TimeUnit.MILLISECONDS);

        // parse the responses
        Map<Address, Object> responseValues = new HashMap<Address, Object>(transport.getMembers().size());
        for (Map.Entry<Address, Response> entry : responseMap.entrySet()) {
            Address address = entry.getKey();
            Response response = entry.getValue();
            if (!response.isSuccessful()) {
                Throwable cause = response instanceof ExceptionResponse ? ((ExceptionResponse) response).getException() : null;
                throw new CacheException("Unsuccessful response received from node " + address + ": " + response, cause);
            }
            responseValues.put(address, ((SuccessfulResponse) response).getResponseValue());
        }

        // now wait for the local command
        Response localResponse = (Response) localFuture.get(timeout, TimeUnit.MILLISECONDS);
        if (!localResponse.isSuccessful()) {
            throw new CacheException("Unsuccessful local response");
        }
        responseValues.put(transport.getAddress(), ((SuccessfulResponse) localResponse).getResponseValue());

        return responseValues;
    }
}
