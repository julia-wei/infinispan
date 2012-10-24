package org.infinispan.xsite.statetransfer;

import java.util.Set;

/**
 *
 */
//@Scope(Scopes.GLOBAL)
public interface XSiteStateTransferManager {

    Set<XSiteStateTransferResponseInfo> pushState(String siteName, String cacheName) throws Exception;

    Set<XSiteStateTransferResponseInfo> pushState(String siteName) throws Exception;

    void cancelStateTransfer(String siteName, String cacheName) throws Exception;


}
