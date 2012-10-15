package org.infinispan.xsite.statetransfer;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.TransactionInfo;

import java.util.List;

/**
 *
 */
@Scope(Scopes.NAMED_CACHE)
public interface XSiteStateProvider {
    List<TransactionInfo> getTransactionsForCache(String cacheName, String siteName, Address address);

    Object startXSiteStateTransfer(String siteName, String cacheName, Address origin);
}
