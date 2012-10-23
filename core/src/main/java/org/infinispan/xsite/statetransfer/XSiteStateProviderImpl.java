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

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.TransactionInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;

import java.util.*;
import java.util.concurrent.ExecutorService;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 *
 */
public class XSiteStateProviderImpl implements XSiteStateProvider {

    private static final Log log = LogFactory.getLog(XSiteStateProviderImpl.class);
    private static final boolean trace = log.isTraceEnabled();

    private LocalTopologyManager localTopologyManager;
    private RpcManager rpcManager;
    private ConsistentHash readCh;
    private Configuration configuration;
    private TransactionTable transactionTable;
    private Transport transport;
    private DataContainer dataContainer;
    private CacheLoaderManager cacheLoaderManager;
    private ExecutorService executorService;
    private long timeout;
    private int chunkSize;

    /**
     * A map that keeps track of current XSite state transfers by Site address.
     */
    private final Map<SiteCachePair, XSiteOutBoundStateTransferTask> transfersBySite = new HashMap<SiteCachePair, XSiteOutBoundStateTransferTask>();


    @Inject
    public void init(
            LocalTopologyManager localTopologyManager,
            @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService executorService,
            RpcManager rpcManager, Configuration configuration,
            TransactionTable transactionTable,
            Transport transport, DataContainer dataContainer, CacheLoaderManager cacheLoaderManager) {

        this.rpcManager = rpcManager;
        this.localTopologyManager = localTopologyManager;
        //TODO confirm if we can inject it here
        this.configuration = configuration;
        this.transactionTable = transactionTable;
        this.transport = transport;
        this.dataContainer = dataContainer;
        this.cacheLoaderManager = cacheLoaderManager;
        this.executorService = executorService;
        //TODO get it from the site configuration
        int chunkSize = configuration.clustering().stateTransfer().chunkSize();
        this.chunkSize = chunkSize > 0 ? chunkSize : Integer.MAX_VALUE;
    }

    public boolean isStateTransferInProgress() {
        synchronized (transfersBySite) {
            return !transfersBySite.isEmpty();
        }
    }

    @Override
    public Object startXSiteStateTransfer(String destinationSiteName, String sourceSiteName, String cacheName, Address origin) throws Exception {
        List<TransactionInfo> transactions = getTransactionsForCache(destinationSiteName, cacheName, origin);
        List<XSiteTransactionInfo> transactionInfoList = translateToXSiteTransaction(transactions);
        if (!transactionInfoList.isEmpty()) {
            pushTransacationsToSite(transactionInfoList, destinationSiteName, sourceSiteName, cacheName, origin);
        }
        Object object = startCacheStateTransfer(destinationSiteName, sourceSiteName, cacheName, origin);
        //now send the state transfer complete command
        completeStateTransfer(destinationSiteName, sourceSiteName, cacheName, origin);
        return object;
    }

    @Override
    public void cancelXSiteStateTransfer(String destinationSiteName, String cacheName) throws Exception {
        if (isStateTransferInProgress()) {
            SiteCachePair siteCachePair = new SiteCachePair(cacheName, destinationSiteName);
            synchronized (transfersBySite) {
                XSiteOutBoundStateTransferTask xSiteOutBoundStateTransferTask = transfersBySite.get(siteCachePair);
                if (xSiteOutBoundStateTransferTask != null) {
                    xSiteOutBoundStateTransferTask.cancel();
                }
            }
        } else {
            if (trace) {
                log.tracef("State transfer to the site % for the cache % is not running", destinationSiteName, cacheName);
            }
        }
    }

    private List<XSiteTransactionInfo> translateToXSiteTransaction(List<TransactionInfo> transactionInfo) {
        List<XSiteTransactionInfo> xSiteTransactionInfoList = new ArrayList<XSiteTransactionInfo>();
        if (transactionInfo != null && !transactionInfo.isEmpty()) {
            for (TransactionInfo trInfo : transactionInfo) {
                XSiteTransactionInfo xSiteTransactionInfo = new XSiteTransactionInfo(trInfo.getGlobalTransaction(), trInfo.getModifications());
                xSiteTransactionInfoList.add(xSiteTransactionInfo);
            }
        }
        return xSiteTransactionInfoList;
    }


    private Set<Object> startCacheStateTransfer(String destinationSiteName, String sourceSiteName, String cacheName, Address origin) {
        log.tracef("Starting outbound transfer of cache  %s to site", cacheName,
                destinationSiteName);

        //TODO need to get the timeout for the xsite state transfer or use the replication timeout
        timeout = configuration.clustering().stateTransfer().timeout();
        XSiteOutBoundStateTransferTask xSiteOutBoundStateTransferTask = new XSiteOutBoundStateTransferTask(destinationSiteName, sourceSiteName, this, dataContainer, cacheLoaderManager, configuration, cacheName, origin, transport, timeout, chunkSize);
        addXSiteStateTransfer(xSiteOutBoundStateTransferTask);
        xSiteOutBoundStateTransferTask.execute(executorService);
        Set<Object> transferredKeys = null;
        if (xSiteOutBoundStateTransferTask.isDone()) {
            transferredKeys = xSiteOutBoundStateTransferTask.getTransferredKeys();
            onTaskCompletion(xSiteOutBoundStateTransferTask);
        }
        return transferredKeys;
    }


    private List<TransactionInfo> getTransactionsForCache(String cacheName, String siteName, Address address) {

        if (trace) {
            log.tracef("Received request for cross site transfer of transactions from node %s for site name %s for cache %s", address, siteName, cacheName);
        }

        CacheTopology cacheTopology = localTopologyManager.getCacheTopology(cacheName);


        readCh = cacheTopology.getCurrentCH();
        if (readCh == null) {
            throw new IllegalStateException("No cache topology received yet");  // no commands are processed until the join is complete, so this cannot normally happen
        }

        Set<Integer> ownedSegments = readCh.getSegmentsForOwner(rpcManager.getAddress());
        List<TransactionInfo> transactions = new ArrayList<TransactionInfo>();
        //we migrate locks only if the cache is transactional and distributed
        if (configuration.transaction().transactionMode().isTransactional()) {
            collectTransactionsToTransfer(transactions, transactionTable.getRemoteTransactions(), ownedSegments);
            collectTransactionsToTransfer(transactions, transactionTable.getLocalTransactions(), ownedSegments);
            if (trace) {
                log.tracef("Found %d transaction(s) to transfer", transactions.size());
            }
        }
        return transactions;

    }

    private void pushTransacationsToSite(List<XSiteTransactionInfo> transactionInfo, String destinationSiteName, String sourceSiteName, String cacheName, Address origin) throws Exception {

        XSiteTransferCommand xSiteTransferCommand = new XSiteTransferCommand(XSiteTransferCommand.Type.TRANSACTION_TRANSFERRED, origin, cacheName, sourceSiteName, null, transactionInfo);
        List<XSiteBackup> backupInfo = new ArrayList<XSiteBackup>(1);
        BackupConfiguration bc = getBackupConfigurationForSite(destinationSiteName);
        if (bc == null) {

            if (trace) {
                log.tracef("No backup configuration is found for the site %s", destinationSiteName);
            }
        }
        boolean isSync = bc.strategy() == BackupConfiguration.BackupStrategy.SYNC;
        XSiteBackup bi = new XSiteBackup(bc.site(), isSync, bc.replicationTimeout());
        backupInfo.add(bi);

        BackupResponse backupResponse = transport.backupRemotely(backupInfo, xSiteTransferCommand);
        backupResponse.waitForBackupToFinish();
        Map<String, Throwable> failedBackups = backupResponse.getFailedBackups();
        if (failedBackups != null && !failedBackups.isEmpty()) {
            //TODO what needs to be done here; do we need to do the same that is being done in BackupSenderImpl
        }
    }

    private void completeStateTransfer(String destinationSiteName, String sourceSiteName, String cacheName, Address origin) throws Exception {

        XSiteTransferCommand xSiteTransferCommand = new XSiteTransferCommand(XSiteTransferCommand.Type.STATE_TRANFER_COMPLETED, origin, cacheName, sourceSiteName, null, null);
        List<XSiteBackup> backupInfo = new ArrayList<XSiteBackup>(1);
        BackupConfiguration bc = getBackupConfigurationForSite(destinationSiteName);
        if (bc == null) {

            if (trace) {
                log.tracef("No backup configuration is found for the site %s", destinationSiteName);
            }
        }
        boolean isSync = bc.strategy() == BackupConfiguration.BackupStrategy.SYNC;
        XSiteBackup bi = new XSiteBackup(bc.site(), isSync, bc.replicationTimeout());
        backupInfo.add(bi);

        BackupResponse backupResponse = transport.backupRemotely(backupInfo, xSiteTransferCommand);
        backupResponse.waitForBackupToFinish();
        Map<String, Throwable> failedBackups = backupResponse.getFailedBackups();

    }

    private void collectTransactionsToTransfer(List<TransactionInfo> transactionsToTransfer,
                                               Collection<? extends CacheTransaction> transactions,
                                               Set<Integer> segments) {

        for (CacheTransaction tx : transactions) {
            // transfer only locked keys that belong to requested segments, located on local node
            Set<Object> lockedKeys = new HashSet<Object>();
            for (Object key : tx.getLockedKeys()) {
                if (segments.contains(readCh.getSegment(key))) {
                    lockedKeys.add(key);
                }
            }
            for (Object key : tx.getBackupLockedKeys()) {
                if (segments.contains(readCh.getSegment(key))) {
                    lockedKeys.add(key);
                }
            }
            if (!lockedKeys.isEmpty()) {
                List<WriteCommand> txModifications = tx.getModifications();
                WriteCommand[] modifications = null;
                if (txModifications != null) {
                    modifications = txModifications.toArray(new WriteCommand[txModifications.size()]);
                }
                transactionsToTransfer.add(new TransactionInfo(tx.getGlobalTransaction(), tx.getViewId(), modifications, lockedKeys));
            }
        }
    }

    private void addXSiteStateTransfer(XSiteOutBoundStateTransferTask xSiteOutBoundStateTransferTask) {
        if (trace) {
            log.tracef("Adding outbound Xsite transfer task for site %s from node %s", xSiteOutBoundStateTransferTask.getDestinationSiteName(), xSiteOutBoundStateTransferTask.getSource());
        }
        synchronized (transfersBySite) {
            SiteCachePair siteCachePair = new SiteCachePair(xSiteOutBoundStateTransferTask.getCacheName(), xSiteOutBoundStateTransferTask.getDestinationSiteName());

            transfersBySite.put(siteCachePair, xSiteOutBoundStateTransferTask);
        }


    }

    public BackupConfiguration getBackupConfigurationForSite(String siteName) {

        for (BackupConfiguration bc : configuration.sites().inUseBackups()) {
            if (bc.site().equals(siteName)) {
                return bc;
            }
        }
        return null;
    }


    private void removeTransfer(XSiteOutBoundStateTransferTask transferTask) {
        synchronized (transfersBySite) {
            if (transferTask != null && !transfersBySite.isEmpty()) {
                SiteCachePair siteCachePair = new SiteCachePair(transferTask.getCacheName(), transferTask.getDestinationSiteName());
                transfersBySite.remove(siteCachePair);
            }
        }
    }

    public void onTaskCompletion(XSiteOutBoundStateTransferTask transferTask) {
        if (trace) {
            //TODO message regarding the cancellation or completion of state transfer task
            log.tracef("Removing outBoundXSiteTransferTask from the node %s to %s",
                    transferTask.isCancelled() ? "cancelled" : "completed", transferTask.getSource(), transferTask.getDestinationSiteName());
        }

        removeTransfer(transferTask);
    }


}
