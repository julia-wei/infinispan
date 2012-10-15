package org.infinispan.xsite.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import static org.infinispan.context.Flag.*;

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


    @Inject
    public void init(Cache cache,

                     InterceptorChain interceptorChain,
                     InvocationContextContainer icc,
                     Configuration configuration,
                     RpcManager rpcManager,
                     CommandsFactory commandsFactory
    ) {
        this.cacheName = cache.getName();

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

        timeout = configuration.clustering().stateTransfer().timeout();
    }


    @Override
    public Object applyState(Address sender, Collection<InternalCacheEntry> cacheEntries) {
        return doApplyState(sender, cacheEntries);
    }

    private Object doApplyState(Address sender, Collection<InternalCacheEntry> cacheEntries) {
        log.debugf("Applying new state for Xsite transfer from %s: received %d cache entries", sender, cacheEntries.size());

        if (trace) {
            List<Object> keys = new ArrayList<Object>(cacheEntries.size());
            for (InternalCacheEntry e : cacheEntries) {
                keys.add(e.getKey());
            }
            log.tracef("Received keys: %s", keys);
        }

        EnumSet<Flag> flags = EnumSet.of(IGNORE_RETURN_VALUES, SKIP_SHARED_CACHE_STORE, SKIP_LOCKING, SKIP_OWNERSHIP_CHECK, SKIP_XSITE_BACKUP);
        for (InternalCacheEntry e : cacheEntries) {
            InvocationContext ctx = icc.createRemoteInvocationContext(sender);
            // locking not necessary as during rehashing we block all transactions
            try {
                PutKeyValueCommand put = useVersionedPut ?
                        commandsFactory.buildVersionedPutKeyValueCommand(e.getKey(), e.getValue(), e.getLifespan(), e.getMaxIdle(), e.getVersion(), flags)
                        : commandsFactory.buildPutKeyValueCommand(e.getKey(), e.getValue(), e.getLifespan(), e.getMaxIdle(), flags);
                put.setPutIfAbsent(true);
                interceptorChain.invoke(ctx, put);
            } catch (Exception ex) {
                log.problemApplyingStateForKey(ex.getMessage(), e.getKey());
            }
        }
        //TODO need to determine which object to return here
        return null;
    }

}
