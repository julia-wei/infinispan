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

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.concurrent.AggregatingNotifyingFutureBuilder;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

/**
 *
 */

public class XSiteOutBoundStateTransferTask implements Runnable {

    private static final Log log = LogFactory.getLog(XSiteOutBoundStateTransferTask.class);

    private final boolean trace = log.isTraceEnabled();

    private XSiteStateProviderImpl xSiteStateProvider;


    private final Configuration configuration;

    private final DataContainer dataContainer;

    private final CacheLoaderManager cacheLoaderManager;


    private final long timeout;

    private final Address source;


    private final String cacheName;

    private final int xsiteTransferChunkSize;

    private int accumulatedEntries;

    private List<InternalCacheEntry> currentLoadOfEntries = new ArrayList<InternalCacheEntry>();
    private Transport transport;

    private String siteName;
    private BackupConfiguration bc;


    /**
     * This is used with RpcManager.invokeRemotelyInFuture() to be able to cancel message sending if the task needs to be canceled.
     */
    private final NotifyingNotifiableFuture<Object> sendFuture = new AggregatingNotifyingFutureBuilder(null);

    /**
     * The Future obtained from submitting this task to an executor service. This is used for cancellation.
     */
    private FutureTask runnableFuture;
    private Set<Object> transferredKeys = new HashSet<Object>();

    public XSiteOutBoundStateTransferTask(String siteName,
                                          XSiteStateProviderImpl xSiteStateProvider, DataContainer dataContainer,
                                          CacheLoaderManager cacheLoaderManager,Configuration configuration,
                                          String cacheName, Address source, Transport transport, long timeout, int xsiteTransferChunkSize) {

        if (siteName == null) {
            throw new IllegalArgumentException("The destination Site cannot be null");
        }

        this.xSiteStateProvider = xSiteStateProvider;
        this.source = source;
        this.dataContainer = dataContainer;
        this.cacheLoaderManager = cacheLoaderManager;

        this.configuration = configuration;
        this.timeout = timeout;
        this.cacheName = cacheName;
        this.xsiteTransferChunkSize = xsiteTransferChunkSize;
        this.transport = transport;
        this.siteName = siteName;
        this.bc = getBackupConfigurationForSite(this.siteName);
    }

    public void execute(ExecutorService executorService) {
        if (runnableFuture != null) {
            throw new IllegalStateException("This task was already submitted");
        }
        runnableFuture = new FutureTask<Void>(this, null) {
            @Override
            protected void done() {

                xSiteStateProvider.onTaskCompletion(XSiteOutBoundStateTransferTask.this);
            }
        };
        executorService.submit(runnableFuture);
    }


    private BackupConfiguration getBackupConfigurationForSite(String siteName) {

        for (BackupConfiguration bc : configuration.sites().inUseBackups()) {
            if (bc.site().equals(siteName)) {
                return bc;
            }
        }
        return null;
    }


    //todo [anistor] check thread interrupt status in loops to implement faster cancellation

    public void run() {
        try {
            // send data container entries
            List<InternalCacheEntry> listOfEntriesToSend = new ArrayList<InternalCacheEntry>();
            for (InternalCacheEntry ice : dataContainer) {
                sendEntry(ice);
            }

            // send cache store entries if needed
            CacheStore cacheStore = getCacheStore();
            if (cacheStore != null) {
                try {

                    Set<Object> storedKeys = cacheStore.loadAllKeys(new ReadOnlyDataContainerBackedKeySet(dataContainer));
                    for (Object key : storedKeys) {

                        try {
                            InternalCacheEntry ice = cacheStore.load(key);
                            if (ice != null) { // check entry still exists
                                sendEntry(ice);
                            }
                        } catch (CacheLoaderException e) {
                            log.failedLoadingValueFromCacheStore(key, e);
                        }
                    }

                } catch (CacheLoaderException e) {
                    log.failedLoadingKeysFromCacheStore(e);
                }
            } else {
                if (trace) {
                    log.tracef("No cache store or the cache store is shared, no need to send any stored cache entries for cache: %s", cacheName);
                }
            }

            // send all the remaining entries in one shot
            sendEntries(true);
        } catch (Throwable t) {
            // ignore eventual exceptions caused by cancellation (have InterruptedException as the root cause)
            if (!runnableFuture.isCancelled()) {
                log.error("Failed to execute outbound transfer", t);
            }
        }
        if (trace) {
            log.tracef("Outbound transfer of keys to remote %s is complete", siteName);
        }
    }

    public String getSiteName() {
        return siteName;
    }

    /**
     * Obtains the CacheStore that will be used for pulling segments that will be sent to other new owners on request.
     * The CacheStore is ignored if it is disabled or if it is shared or if fetchPersistentState is disabled.
     */
    private CacheStore getCacheStore() {
        if (cacheLoaderManager != null && cacheLoaderManager.isEnabled() && !cacheLoaderManager.isShared() && cacheLoaderManager.isFetchPersistentState()) {
            return cacheLoaderManager.getCacheStore();
        }
        return null;
    }

    public Address getSource() {
        return source;
    }

    public String getCacheName() {
        return cacheName;
    }

    private void sendEntry(InternalCacheEntry ice) throws Exception {
        // send if we have a full chunk
        if (accumulatedEntries >= xsiteTransferChunkSize) {
            sendEntries(false);
            currentLoadOfEntries.clear();
            accumulatedEntries = 0;
        }
        currentLoadOfEntries.add(ice);
        accumulatedEntries++;
    }


    private void sendEntries(boolean isLastLoad) throws Exception {
        if (!currentLoadOfEntries.isEmpty()) {

            XSiteTransferCommand xSiteTransferCommand = new XSiteTransferCommand(XSiteTransferCommand.Type.STATE_TRANSFERRED, source, currentLoadOfEntries, cacheName, null);
            List<XSiteBackup> backupInfo = new ArrayList<XSiteBackup>(1);
            if (bc == null) {
               
                bc = xSiteStateProvider.getBackupConfigurationForSite(siteName);
            }
            boolean isSync = bc.strategy() == BackupConfiguration.BackupStrategy.SYNC;
            XSiteBackup bi = new XSiteBackup(bc.site(), isSync, bc.replicationTimeout());
            backupInfo.add(bi);

            transport.backupRemotely(backupInfo, xSiteTransferCommand);


        }
        if (isLastLoad) {
            currentLoadOfEntries.clear();
            accumulatedEntries = 0;
        }
    }


    private void calculateTransferredKeys(List<InternalCacheEntry> transferredEntries) {
        if (transferredEntries != null) {
            for (InternalCacheEntry ie : transferredEntries) {
                transferredKeys.add(ie.getKey());
            }
        }
    }

    /**
     * Cancel the whole task.
     */
    public void cancel() {
        if (runnableFuture != null && !runnableFuture.isCancelled()) {
            runnableFuture.cancel(true);
            sendFuture.cancel(true);
        }
    }

    public boolean isCancelled() {
        return runnableFuture != null && runnableFuture.isCancelled();
    }
}
