package org.infinispan.xsite.statetransfer;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 *
 */
@Scope(Scopes.GLOBAL)
public interface XSiteStateTransferManager {

     void pushState(String siteName, String cacheName);
     void pushState(String siteName);


}
