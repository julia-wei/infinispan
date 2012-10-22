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

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.BackupReceiverRepository;

import java.util.List;


public class XSiteTransferCommand implements ReplicableCommand {

    private static final Log log = LogFactory.getLog(XSiteTransferCommand.class);
    private List<XSiteTransactionInfo> transactionInfo;
    private Address origin;
    private List<InternalCacheEntry> internalCacheEntries;
    private String cacheName;
    private Type type;
    private String originSiteName;


    private transient BackupReceiverRepository backupReceiverRepository;

    public enum Type {

        TRANSACTION_TRANSFERRED,

        STATE_TRANSFERRED,

    }


    public XSiteTransferCommand(Type type, Address origin, String cacheName, String originSiteName, List<InternalCacheEntry> internalCacheEntries, List<XSiteTransactionInfo> transactionInfo) {
        this.origin = origin;
        this.internalCacheEntries = internalCacheEntries;
        this.cacheName = cacheName;
        this.transactionInfo = transactionInfo;
        this.type = type;
        this.originSiteName = originSiteName;
    }

    @Inject
    public void init(BackupReceiverRepository backupReceiverRepository) {
        this.backupReceiverRepository = backupReceiverRepository;
    }


    @Override
    public Object perform(InvocationContext ctx) throws Throwable {
        final boolean trace = log.isTraceEnabled();
        LogFactory.pushNDC(cacheName, trace);
        XSiteStateTransferReceiver xSiteStateTransferReceiver = backupReceiverRepository.getXSiteStateTransferReceiver(originSiteName, cacheName);
        try {
            switch (type) {
                case STATE_TRANSFERRED:

                    return xSiteStateTransferReceiver.applyState(origin, internalCacheEntries);

                case TRANSACTION_TRANSFERRED:
                    return xSiteStateTransferReceiver.applyTransactions(transactionInfo, cacheName);


                default:
                    throw new CacheException("Unknown state request command type: " + type);
            }
        } finally {
            LogFactory.popNDC(trace);
        }
    }

    @Override
    public byte getCommandId() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public List<XSiteTransactionInfo> getTransactionInfo() {
        return transactionInfo;
    }

    public Address getOrigin() {
        return origin;
    }

    public List<InternalCacheEntry> getInternalCacheEntries() {
        return internalCacheEntries;
    }

    public String getCacheName() {
        return cacheName;
    }

    public Type getType() {
        return type;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{(byte) type.ordinal(), getOrigin(), cacheName, internalCacheEntries, transactionInfo};
    }

    public void setTransactionInfo(List<XSiteTransactionInfo> transactionInfo) {
        this.transactionInfo = transactionInfo;
    }

    public void setOrigin(Address origin) {
        this.origin = origin;
    }

    public void setInternalCacheEntries(List<InternalCacheEntry> internalCacheEntries) {
        this.internalCacheEntries = internalCacheEntries;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public void setType(Type type) {
        this.type = type;
    }

    @Override
    public void setParameters(int commandId, Object[] parameters) {
        int i = 0;
        type = Type.values()[(Byte) parameters[i++]];
        setOrigin((Address) parameters[i++]);
        setCacheName((String) parameters[i++]);
        setInternalCacheEntries((List<InternalCacheEntry>) parameters[i++]);
        setTransactionInfo((List<XSiteTransactionInfo>) parameters[i++]);
    }

    @Override
    public boolean isReturnValueExpected() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
