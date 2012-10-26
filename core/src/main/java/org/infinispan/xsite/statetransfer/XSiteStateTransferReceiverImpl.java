package org.infinispan.xsite.statetransfer;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.*;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.GlobalTransactionInfo;

import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**

 */
public class XSiteStateTransferReceiverImpl implements XSiteStateTransferReceiver {
    private static final Log log = LogFactory.getLog(XSiteStateTransferReceiver.class);
    private static final boolean trace = log.isTraceEnabled();
    private String cacheName;
    private Configuration configuration;
    private RpcManager rpcManager;
    private CommandsFactory commandsFactory;

    private InterceptorChain interceptorChain;
    private InvocationContextContainer icc;

    private long timeout;
    private boolean useVersionedPut;
    private boolean fetchEnabled;
    private final BackupReceiver backupReceiver;
    private final XSiteBackupCacheUpdater xSiteBackupCacheUpdater;


    public XSiteStateTransferReceiverImpl(BackupReceiver backupReceiver) {
        this.cacheName = backupReceiver.getCache().getName();
        this.backupReceiver = backupReceiver;
        xSiteBackupCacheUpdater = new XSiteBackupCacheUpdater(backupReceiver.getCache(), backupReceiver);

    }


    @Inject
    public void init(InterceptorChain interceptorChain,
                     InvocationContextContainer icc,
                     Configuration configuration,
                     CommandsFactory commandsFactory

    ) {

        this.interceptorChain = interceptorChain;
        this.icc = icc;
        this.configuration = configuration;
        this.rpcManager = rpcManager;
        this.commandsFactory = commandsFactory;


        // we need to use a special form of PutKeyValueCommand that can apply versions too
        useVersionedPut = configuration.transaction().transactionMode().isTransactional() &&
                configuration.versioning().enabled() &&
                configuration.locking().writeSkewCheck() &&
                configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC &&
                configuration.clustering().cacheMode().isClustered();
        //TODO get it form the new configuration
        timeout = configuration.clustering().stateTransfer().timeout();
    }


    @Override
    public Object applyState(Address sender, Collection<InternalCacheEntry> cacheEntries) {
        return xSiteBackupCacheUpdater.doApplyState(sender, cacheEntries);
    }

    @Override
    public Object applyTransactions(List<XSiteTransactionInfo> transactionInfo, String cacheName) throws Throwable {
        for (XSiteTransactionInfo xSiteTransactionInfo : transactionInfo) {
            handleSingleTransaction(xSiteTransactionInfo);
        }

        return null;
    }

    @Override
    public void stateTransferCompleted() {
        backupReceiver.stateTransferCompleted();
    }

    private void handleSingleTransaction(XSiteTransactionInfo xSiteTransactionInfo) throws Throwable {
        xSiteBackupCacheUpdater.replayModifications(xSiteTransactionInfo);
    }


    public static final class XSiteBackupCacheUpdater extends AbstractVisitor {

        private static Log log = LogFactory.getLog(XSiteBackupCacheUpdater.class);


        private final AdvancedCache backupCache;
        private final BackupReceiver backupReceiver;

        XSiteBackupCacheUpdater(Cache backup, BackupReceiver backupReceiver) {
            //ignore return values on the backup
            this.backupCache = backup.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES, Flag.SKIP_XSITE_BACKUP);
            this.backupReceiver = backupReceiver;

        }

        @Override
        public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
            log.tracef("Processing a remote put %s", command);
            if (command.isConditional()) {
                return backupCache.putIfAbsent(command.getKey(), command.getValue(),
                        command.getLifespanMillis(), TimeUnit.MILLISECONDS,
                        command.getMaxIdleTimeMillis(), TimeUnit.MILLISECONDS);
            }
            return backupCache.put(command.getKey(), command.getValue(),
                    command.getLifespanMillis(), TimeUnit.MILLISECONDS,
                    command.getMaxIdleTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
            if (command.isConditional()) {
                return backupCache.remove(command.getKey(), command.getValue());
            }
            return backupCache.remove(command.getKey());
        }

        @Override
        public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
            if (command.isConditional() && command.getOldValue() != null) {
                return backupCache.replace(command.getKey(), command.getOldValue(), command.getNewValue(),
                        command.getLifespanMillis(), TimeUnit.MILLISECONDS,
                        command.getMaxIdleTimeMillis(), TimeUnit.MILLISECONDS);
            }
            return backupCache.replace(command.getKey(), command.getNewValue(),
                    command.getLifespanMillis(), TimeUnit.MILLISECONDS,
                    command.getMaxIdleTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
            backupCache.putAll(command.getMap(), command.getLifespanMillis(), TimeUnit.MILLISECONDS,
                    command.getMaxIdleTimeMillis(), TimeUnit.MILLISECONDS);
            return null;
        }

        @Override
        public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
            backupCache.clear();
            return null;
        }


        private TransactionManager txManager() {
            return backupCache.getAdvancedCache().getTransactionManager();
        }

        public TransactionTable txTable() {
            return backupCache.getComponentRegistry().getComponent(TransactionTable.class);
        }

        private void replayModifications(XSiteTransactionInfo xSiteTransactionInfo) throws Throwable {

            GlobalTransactionInfo globalTransactionInfoFromPreviousCommit = checkForCommitOrRollBackReceivedBeforePrepare(xSiteTransactionInfo.getGlobalTransaction());
            if (globalTransactionInfoFromPreviousCommit != null) {
                if (globalTransactionInfoFromPreviousCommit.getTransactionStatus() == GlobalTransactionInfo.TransactionStatus.COMMIT_RECEIVED) {
                    completeTransaction(xSiteTransactionInfo, true);
                } else {
                    completeTransaction(xSiteTransactionInfo, false);
                }
                return;
            }
            TransactionManager tm = txManager();

            try {
                tm.begin();
                applyModifications(xSiteTransactionInfo.getModifications());

            }
            finally {
                LocalTransaction localTx = txTable().getLocalTransaction(tm.getTransaction());
                localTx.setFromRemoteSite(true);
                GlobalTransactionInfo globalTransactionInfo = new GlobalTransactionInfo(localTx.getGlobalTransaction(), GlobalTransactionInfo.TransactionStatus.PREPARED_RECEIVED);
                backupReceiver.addGlobalTransaction(xSiteTransactionInfo.getGlobalTransaction(), globalTransactionInfo);
                tm.suspend();

            }
        }

        private void completeTransaction(XSiteTransactionInfo xSiteTransactionInfo, boolean commit) throws Throwable {
            TransactionManager tm = txManager();
            try {
                tm.begin();
                applyModifications(xSiteTransactionInfo.getModifications());

            }
            finally {
                LocalTransaction localTx = txTable().getLocalTransaction(tm.getTransaction());
                localTx.setFromRemoteSite(true);
                if (commit) {
                    tm.commit();
                } else {
                    tm.rollback();
                }
            }
        }

        private void applyModifications(WriteCommand[] modifications) throws Throwable {
            for (WriteCommand c : modifications) {
                c.acceptVisitor(null, this);
            }

        }

        private GlobalTransactionInfo checkForCommitOrRollBackReceivedBeforePrepare(GlobalTransaction globalTransaction) {
            GlobalTransactionInfo globalTransactionInfo = backupReceiver.getGlobalTransactionInfo(globalTransaction);
            if (globalTransactionInfo != null &&
                    (globalTransactionInfo.getTransactionStatus() == GlobalTransactionInfo.TransactionStatus.COMMIT_RECEIVED ||
                            globalTransactionInfo.getTransactionStatus() == GlobalTransactionInfo.TransactionStatus.ROLLBACK_RECEIVED)) {
                backupReceiver.removeGlobalTransaction(globalTransaction);
                return globalTransactionInfo;
            }
            return null;
        }

        public Object doApplyState(Address sender, Collection<InternalCacheEntry> cacheEntries) {
            log.debugf("Applying new state for Xsite transfer from %s: received %d cache entries", sender, cacheEntries.size());

            if (trace) {
                List<Object> keys = new ArrayList<Object>(cacheEntries.size());
                for (InternalCacheEntry e : cacheEntries) {
                    keys.add(e.getKey());
                }
                log.tracef("Received keys: %s", keys);
            }
            for (InternalCacheEntry e : cacheEntries) {

                return backupCache.putIfAbsent(e.getKey(), e.getValue(),
                        e.getLifespan(), TimeUnit.MILLISECONDS,
                        e.getMaxIdle(), TimeUnit.MILLISECONDS);

            }
            return null;
        }

    }
}