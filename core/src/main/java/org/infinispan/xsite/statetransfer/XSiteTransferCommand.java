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

import java.util.List;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.TransactionInfo;


public class XSiteTransferCommand implements ReplicableCommand {

    private List<TransactionInfo> transactionInfo;
    private Address origin;
    private List<InternalCacheEntry> internalCacheEntries;
    private String cacheName;

    public enum Type {

        TRANSACTION_TRANSFERRED,

        STATE_TRANSFERRED,

    }


    public XSiteTransferCommand(Address origin, List<InternalCacheEntry> internalCacheEntries, String cacheName, List<TransactionInfo> transactionInfo) {
        this.origin = origin;
        this.internalCacheEntries = internalCacheEntries;
        this.cacheName = cacheName;
        this.transactionInfo = transactionInfo;
    }

    @Override
    public Object perform(InvocationContext ctx) throws Throwable {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public byte getCommandId() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setParameters(int commandId, Object[] parameters) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isReturnValueExpected() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
