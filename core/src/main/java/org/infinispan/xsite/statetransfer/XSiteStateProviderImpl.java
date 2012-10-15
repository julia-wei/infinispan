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
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.TransactionInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;

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

    @Inject
    public void init(
            LocalTopologyManager localTopologyManager, RpcManager rpcManager, Configuration configuration, TransactionTable transactionTable) {

        this.rpcManager = rpcManager;
        this.localTopologyManager = localTopologyManager;
        //TODO confirm if we can inject it here
        this.configuration = configuration;
        this.transactionTable = transactionTable;


    }

    public List<TransactionInfo> getTransactionsForCache(String cacheName, String siteName, Address address) {

        if (trace) {
            log.tracef("Received request for cross site transfer of transactions from node %s for site name %s for cache %s", address, siteName, cacheName);
        }

        if (readCh == null) {
            throw new IllegalStateException("No cache topology received yet");  // no commands are processed until the join is complete, so this cannot normally happen
        }
        CacheTopology cacheTopology = localTopologyManager.getCacheTopology(cacheName);
        //TODO which cache to get here
        readCh = cacheTopology.getCurrentCH();
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

    private void pushTransacationsToXsite(List<TransactionInfo> transactionInfo) {

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

    private void removeTransfer(XSiteOutBoundStateTransferTask transferTask) {
        //TODO implement any cleanup task here
    }

    public void onTaskCompletion(XSiteOutBoundStateTransferTask transferTask) {
        if (trace) {
            //TODO message regarding the cancellation or completion of state transfer task
//          log.tracef("Removing %s outbound transfer of segments %s to %s",
//                transferTask.isCancelled() ? "cancelled" : "completed", transferTask.getSegments(), transferTask.getDestination());
        }

        removeTransfer(transferTask);
    }


}
