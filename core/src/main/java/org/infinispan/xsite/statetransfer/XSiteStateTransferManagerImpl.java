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

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 *
 */
public class XSiteStateTransferManagerImpl implements XSiteStateTransferManager {

    private static Log log = LogFactory.getLog(XSiteStateTransferManagerImpl.class);
    private static final boolean trace = log.isTraceEnabled();
    private Transport transport;
    private ExecutorService asyncTransportExecutor;
    private GlobalComponentRegistry gcr;
    private String cacheName;
    private LocalTopologyManager localTopologyManager;
    private Configuration config;
    private BackupConfiguration bc;
    private boolean xsiteTransferRunning;


    @Inject
    public void inject(Transport transport,
                       @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor,
                       GlobalComponentRegistry gcr,
                       Cache cache) {
        this.transport = transport;
        this.asyncTransportExecutor = asyncTransportExecutor;
        this.gcr = gcr;
        cacheName = cache.getName();
        this.localTopologyManager = localTopologyManager;
        this.config = cache.getCacheConfiguration();
    }

    @Override
    public void pushState(String siteName) {


        pushState(siteName, cacheName);


    }

    private BackupConfiguration getBackupConfigurationForSite(String siteName) {

        for (BackupConfiguration bc : config.sites().inUseBackups()) {
            if (bc.site().equals(siteName)) {
                return bc;
            }
        }
        return null;
    }


    @Override
    public void pushState(String siteName, String cacheName) {
        bc = getBackupConfigurationForSite(siteName);
        if (bc == null) {
            if (trace)
                log.tracef("The current cache %s does not have any backup for the given site %s", cacheName, siteName);
            //TODO do we need to throw an exception here
        }
        xsiteTransferRunning = true;

        Address address = transport.getAddress();
        XSiteStateRequestCommand xsiteStateRequestCommand = buildCommand(siteName, cacheName, address);
        
        //TODO return object to be determined
        //TODO Exception handling to be determined
        try {
            Object object = executeOnClusterSync(xsiteStateRequestCommand, bc.replicationTimeout());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private XSiteStateRequestCommand buildCommand(String siteName, String cacheName, Address address) {
        XSiteStateRequestCommand xSiteStateRequestCommand = new XSiteStateRequestCommand(siteName, cacheName, address, XSiteStateRequestCommand.Type.START_XSITE_TRANSACTION_TRANSFER);
        return xSiteStateRequestCommand;
    }


    private List<String> getCacheNamesForCurrentNode(GlobalComponentRegistry gcr) {
        //TODO is there some way to get the caches from the GlobalcomponentRegistr
        //gcr.get
        return Collections.unmodifiableList(new ArrayList<String>());
    }

    private Map<Address, Object> executeOnClusterSync(final ReplicableCommand command, final long timeout)
            throws Exception {
        // first invoke remotely
        Future<Map<Address, Response>> remoteFuture = asyncTransportExecutor.submit(new Callable<Map<Address, Response>>() {
            @Override
            public Map<Address, Response> call() throws Exception {
                return transport.invokeRemotely(null, command,
                        ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, timeout, true, null);
            }
        });

        // now invoke the command on the local node
        Future<Object> localFuture = asyncTransportExecutor.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                gcr.wireDependencies(command);
                try {
                    return command.perform(null);
                } catch (Throwable t) {
                    throw new Exception(t);
                }
            }
        });


        // wait for the remote commands to finish
        Map<Address, Response> responseMap = remoteFuture.get(timeout, TimeUnit.MILLISECONDS);

        // parse the responses
        Map<Address, Object> responseValues = new HashMap<Address, Object>(transport.getMembers().size());
        for (Map.Entry<Address, Response> entry : responseMap.entrySet()) {
            Address address = entry.getKey();
            Response response = entry.getValue();
            if (!response.isSuccessful()) {
                Throwable cause = response instanceof ExceptionResponse ? ((ExceptionResponse) response).getException() : null;
                throw new CacheException("Unsuccessful response received from node " + address + ": " + response, cause);
            }
            responseValues.put(address, ((SuccessfulResponse) response).getResponseValue());
        }

        // now wait for the local command
        Response localResponse = (Response) localFuture.get(timeout, TimeUnit.MILLISECONDS);
        if (!localResponse.isSuccessful()) {
            throw new CacheException("Unsuccessful local response");
        }
        responseValues.put(transport.getAddress(), ((SuccessfulResponse) localResponse).getResponseValue());

        return responseValues;
    }
}
