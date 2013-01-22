package org.infinispan.xsite.statetransfer;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.TransactionInfo;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
@Scope(Scopes.NAMED_CACHE)
public interface XSiteStateProvider {
    
    long startXSiteStateTransfer(String destinationSiteName, String sourceSiteName, String cacheName, Address origin) throws Exception;
    void cancelXSiteStateTransfer(String destinationSiteName, String cacheName) throws Exception;
    Long getTransferredKeys(String destinationName, String cacheName);
}
