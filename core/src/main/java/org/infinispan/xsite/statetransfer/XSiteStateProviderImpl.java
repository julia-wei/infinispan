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

import org.infinispan.Cache;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.TransactionInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.concurrent.AggregatingNotifyingFutureBuilder;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;

import java.util.*;
import java.util.concurrent.*;

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
    private long numOfKeysTransferred;
    private EmbeddedCacheManager embeddedCacheManager;
    private ConcurrentMap<SiteCachePair, Long> keysTransferredMap = new ConcurrentHashMap<SiteCachePair, Long>();



       /**
     * This is used with RpcManager.invokeRemotelyInFuture() to be able to cancel message sending if the task needs to be canceled.
     */
    private final NotifyingNotifiableFuture<Object> sendFuture = new AggregatingNotifyingFutureBuilder(null);

    /**
     * The Future obtained from submitting this task to an executor service. This is used for cancellation.
     */
    private FutureTask runnableFuture;


    private int accumulatedEntries;

    private List<InternalCacheEntry> currentLoadOfEntries = new ArrayList<InternalCacheEntry>();

    public XSiteStateProviderImpl() {
    }

    @Inject
    public void init(
            LocalTopologyManager localTopologyManager,
            @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService executorService,
            RpcManager rpcManager, Configuration configuration,
            TransactionTable transactionTable,
            Transport transport, DataContainer dataContainer, CacheLoaderManager cacheLoaderManager, EmbeddedCacheManager embeddedCacheManager) {

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
        this.embeddedCacheManager = embeddedCacheManager;
    }

    public boolean isStateTransferInProgress() {
        return runnableFuture != null && !runnableFuture.isCancelled();
    }

    @Override
    public long startXSiteStateTransfer(String destinationSiteName, String sourceSiteName, String cacheName, Address origin) throws Exception {

        List<TransactionInfo> transactions = getTransactionsForCache(destinationSiteName, cacheName, origin);
        List<XSiteTransactionInfo> transactionInfoList = translateToXSiteTransaction(transactions);
        if (!transactionInfoList.isEmpty()) {
            pushTransacationsToSite(transactionInfoList, destinationSiteName, sourceSiteName, cacheName, origin);
        }
        startCacheStateTransfer(destinationSiteName, sourceSiteName, cacheName, origin);

        onCashStateTransferTaskCompletion(destinationSiteName, cacheName, origin);
        //now send the state transfer complete command
        sendStateTransferCompleteCommand(destinationSiteName, sourceSiteName, cacheName, origin);
        long result =  numOfKeysTransferred;
        SiteCachePair siteCachePair = new SiteCachePair(cacheName, sourceSiteName);
        keysTransferredMap.put(siteCachePair, new Long(result)) ;
        return result;
    }

    @Override
    public void cancelXSiteStateTransfer(String destinationSiteName, String cacheName) throws Exception {
        if (isStateTransferInProgress()) {
               runnableFuture.cancel(true);
        } else {
            if (trace) {
                log.tracef("State transfer to the site % for the cache % is not running", destinationSiteName, cacheName);
            }
        }
    }

    @Override
    public Long getTransferredKeys(String destinationName, String cacheName) {
        SiteCachePair siteCachePair = new SiteCachePair(cacheName, destinationName);
        return keysTransferredMap.get(siteCachePair);

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


    private void startCacheStateTransfer(String destinationSiteName, String sourceSiteName, String cacheName, Address origin) {
        log.tracef("Starting outbound transfer of cache  %s to site", cacheName,
                destinationSiteName);

        //TODO need to get the timeout for the xsite state transfer or use the replication timeout
        timeout = configuration.clustering().stateTransfer().timeout();
        if (runnableFuture != null) {
            throw new IllegalStateException("This task was already submitted");
        }
        StateTransferThread thread = new StateTransferThread(destinationSiteName, sourceSiteName, cacheName, origin);
         runnableFuture = new FutureTask<Void>(thread);

        executorService.submit(runnableFuture);

        if(runnableFuture.isDone()){
            return;
        }

    }
   private class StateTransferThread implements Callable {
       private final String destinationName;
        private final String sourceSiteName;
       private final Address origin;
       private final String cacheName;

       public StateTransferThread(String destinationName, String sourceSiteName, String cacheName, Address origin ){
           this.destinationName = destinationName;
           this.sourceSiteName = sourceSiteName;
           this.origin = origin;
           this. cacheName = cacheName;
       }
       public Object call() {
            try {
               sendCacheState(destinationName,sourceSiteName, cacheName, origin );
            } catch (Throwable e) {
               e.printStackTrace();
            }
           return null;
         }
   }

    private void sendCacheState(String destinationSiteName, String sourceSiteName, String cacheName, Address origin) {
         try {
            // send data container entries
            List<InternalCacheEntry> listOfEntriesToSend = new ArrayList<InternalCacheEntry>();
            for (InternalCacheEntry ice : dataContainer) {
                sendEntry(ice, origin, cacheName, sourceSiteName, destinationSiteName);
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
                                sendEntry(ice, origin, cacheName, sourceSiteName, destinationSiteName);
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
            sendEntries(true, origin, cacheName, sourceSiteName, destinationSiteName);
        } catch (Throwable t) {
            // ignore eventual exceptions caused by cancellation (have InterruptedException as the root cause)
            if (!runnableFuture.isCancelled()) {
                log.error("Failed to execute outbound transfer", t);
            }
        }
        if (trace) {
            log.tracef("Outbound transfer of keys to remote %s is complete", destinationSiteName);
        }

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

    private void sendEntry(InternalCacheEntry ice, Address origin, String cacheName, String sourceSiteName, String destinationSiteName) throws Exception {
        // send if we have a full chunk
        if (accumulatedEntries >= chunkSize) {
            sendEntries(false, origin, cacheName, sourceSiteName, destinationSiteName);
            currentLoadOfEntries.clear();
            accumulatedEntries = 0;
        }
        currentLoadOfEntries.add(ice);
        accumulatedEntries++;
    }


    private void sendEntries(boolean isLastLoad, Address origin, String cacheName, String sourceSiteName, String destinationSiteName) throws Exception {
        if (!currentLoadOfEntries.isEmpty()) {


            XSiteTransferCommand xSiteTransferCommand = new XSiteTransferCommand(XSiteTransferCommand.Type.STATE_TRANSFERRED, origin,cacheName, sourceSiteName, currentLoadOfEntries,  null);
            List<XSiteBackup> backupInfo = new ArrayList<XSiteBackup>(1);
            BackupConfiguration bc =  getBackupConfigurationForSite(destinationSiteName);

            boolean isSync = bc.strategy() == BackupConfiguration.BackupStrategy.SYNC;
            XSiteBackup bi = new XSiteBackup(bc.site(), isSync, bc.replicationTimeout());
            backupInfo.add(bi);

            transport.backupRemotely(backupInfo, xSiteTransferCommand);
            calculateTransferredKeys(currentLoadOfEntries);


        }
        if (isLastLoad) {
            currentLoadOfEntries.clear();
            accumulatedEntries = 0;
        }
    }


    private void calculateTransferredKeys(List<InternalCacheEntry> transferredEntries) {
        if (transferredEntries != null) {

                numOfKeysTransferred +=transferredEntries.size();
            }
        }

    private List<TransactionInfo> getTransactionsForCache(String siteName, String cacheName, Address address) {

        if (trace) {
            log.tracef("Received request for cross site transfer of transactions from node %s for site name %s for cache %s", address, siteName, cacheName);
        }

        Cache cache = embeddedCacheManager.getCache(cacheName);
        if(cache.getStatus().isTerminated()){
            cache.start();
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

    private void sendStateTransferCompleteCommand(String destinationSiteName, String sourceSiteName, String cacheName, Address origin) throws Exception {

        XSiteTransferCommand xSiteTransferCommand = new XSiteTransferCommand(XSiteTransferCommand.Type.STATE_TRANSFER_COMPLETED, origin, cacheName, sourceSiteName, null, null);
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


    public BackupConfiguration getBackupConfigurationForSite(String siteName) {

        for (BackupConfiguration bc : configuration.sites().inUseBackups()) {
            if (bc.site().equals(siteName)) {
                return bc;
            }
        }
        return null;
    }


    public void onCashStateTransferTaskCompletion(String destinationSiteName, String cacheName, Address origin) {
        if (trace) {

            log.tracef("The transfer of cache %s to site %s is completed",
                    cacheName, destinationSiteName, origin);
        }
        runnableFuture = null;
        numOfKeysTransferred = 0;
        currentLoadOfEntries.clear();
        accumulatedEntries = 0;
    }


}
