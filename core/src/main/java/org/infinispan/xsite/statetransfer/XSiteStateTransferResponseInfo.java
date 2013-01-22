package org.infinispan.xsite.statetransfer;

import org.infinispan.remoting.transport.Address;

import java.util.Set;


/**
*
*/
public class XSiteStateTransferResponseInfo {
    private final String siteName;
    private final Address nodeAddress;
    private final long numOfKeysTransferred;
    private final String cacheName;

    public XSiteStateTransferResponseInfo(String siteName, Address nodeAddress,long numOfKeysTransferred, String cacheName) {
        this.siteName = siteName;
        this.nodeAddress = nodeAddress;
        this.numOfKeysTransferred = numOfKeysTransferred;
        this.cacheName = cacheName;
    }

    @Override
    public String toString() {
        return "XSiteStateTransferResponseInfo{" +
                "siteName='" + siteName + '\'' +
                ", nodeAddress=" + nodeAddress +
                ", cacheName=" + cacheName +
                ", numOfKeysTransferred=" + numOfKeysTransferred +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        XSiteStateTransferResponseInfo that = (XSiteStateTransferResponseInfo) o;

        if (nodeAddress != null ? !nodeAddress.equals(that.nodeAddress) : that.nodeAddress != null) return false;
        if (siteName != null ? !siteName.equals(that.siteName) : that.siteName != null) return false;
         if (cacheName != null ? !cacheName.equals(that.cacheName) : that.cacheName != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = siteName != null ? siteName.hashCode() : 0;
        result = 31 * result + (nodeAddress != null ? nodeAddress.hashCode() : 0);
        result = 31 * result + (cacheName != null ? cacheName.hashCode() : 0);
        return result;
    }
}
