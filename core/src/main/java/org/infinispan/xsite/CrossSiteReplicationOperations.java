/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.xsite;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.infinispan.xsite.statetransfer.XSiteStateTransferResponseInfo;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.Set;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@MBean(objectName = "XSiteAdmin", description = "Exposes tooling for handling backing up data to remote sites.")
public class CrossSiteReplicationOperations {

   private volatile BackupSender backupSender;
   private volatile XSiteStateTransferManager xSiteStateTransferManager;

   @Inject
   public void init(BackupSender backupSender, XSiteStateTransferManager xSiteStateTransferManager) {
      this.backupSender = backupSender;
      this.xSiteStateTransferManager = xSiteStateTransferManager;
   }

   @Operation(displayName = "Brings the given site back online on this node.")
   @ManagedOperation(description = "Brings the given site back online on this node.")
   public String bringSiteOnline(String siteName) {
      return backupSender.bringSiteOnline(siteName).toString();
   }

   @Operation(displayName = "Push the transactions and state of this site caches to the target site")
   @ManagedOperation(description = "Transfer the state of this site to the target site")
   public Set<XSiteStateTransferResponseInfo> pushState(String siteName) throws Exception {
      return xSiteStateTransferManager.pushState(siteName);
   }

   @Operation(displayName = "Push the transactions and state of this site cache to the target site")
   @ManagedOperation(description = "Transfer the state of this site cache to the target site")
   public  Set<XSiteStateTransferResponseInfo> pushState(String siteName, String cacheName) throws Exception {
      return xSiteStateTransferManager.pushState(siteName, cacheName);
   }

   @Operation(displayName = "Cancel the state transfer to the specified site and the cache")
   @ManagedOperation(description = "Cancel state transfer to the specified site and cache")
   public  void cancelStateTransfer(String siteName, String cacheName) throws Exception {
      xSiteStateTransferManager.cancelStateTransfer(siteName, cacheName);
   }

}
