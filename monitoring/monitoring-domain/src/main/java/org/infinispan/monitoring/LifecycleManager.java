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
package org.infinispan.monitoring;



import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.CustomInterceptorsConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.InterceptorConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.CacheMgmtInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.monitoring.listener.MemoryUsageListener;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Lifecycle of the Monitoring module: add MemoryUsageListener to each cache at 
 * cache starts. 
 * 
 * @author Yan Wei <yiyixiaofan@163.com> (C) 2011 Red Hat Inc.
 */
public class LifecycleManager extends AbstractModuleLifecycle {
   
    private static final Log log = LogFactory.getLog(LifecycleManager.class, Log.class);


    /**
     * Registers the monitoring intercepter in the cache before it gets started
     */
    @Override
    public void cacheStarting(ComponentRegistry cr, Configuration cfg, String cacheName) {
        MemoryUsageListener memoryListener = null;
        if (cfg.jmxStatistics().enabled()) {
            memoryListener = createMonitoringListenerIfNeeded(cr, cfg);
            
            //register cache level listener
            if (memoryListener != null) {            
                 Cache<?, ?> cache = cr.getComponent(Cache.class);
                 cache.addListener(memoryListener);
             }
             log.info("Registered MemoryUsageInterceptor as cache listener");    
        } else {
            log.info("Cache statistic is not enabled"); 
        }       
    }

    private MemoryUsageListener createMonitoringListenerIfNeeded(ComponentRegistry cr, Configuration cfg) {
        MemoryUsageListener memoryListener = cr.getComponent(MemoryUsageListener.class);
        if (memoryListener == null) {
            memoryListener = new MemoryUsageListener();
            // Interceptor registration not needed, core configuration handling
            // already does it for all custom interceptors - UNLESS the InterceptorChain already exists in the component registry!            
            InterceptorChain ic = cr.getComponent(InterceptorChain.class);
            
            if (ic != null && cr != null) {
                cr.registerComponent(memoryListener, MemoryUsageListener.class);
            }
            
            ConfigurationBuilder builder = new ConfigurationBuilder().read(cfg);
            InterceptorConfigurationBuilder interceptorBuilder = builder.customInterceptors().addInterceptor();
            interceptorBuilder.interceptor(memoryListener);

            if (ic != null) {
                ic.addInterceptorAfter(memoryListener, CacheMgmtInterceptor.class);
            }
            interceptorBuilder.after(CacheMgmtInterceptor.class);
            cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
        }
        return memoryListener;
    }


    @Override
    public void cacheStarted(ComponentRegistry cr, String cacheName) {
        Configuration configuration = cr.getComponent(Configuration.class);
        boolean jmsStatEnabled = configuration.jmxStatistics().enabled();
        if ( ! jmsStatEnabled ) {
            if ( verifyChainContainsMonitoringListener(cr) ) {
                throw new IllegalStateException( "It was NOT expected to find the memoery usage listener as JMX statistics was disabled, but it was found" );
            }
            return;
        }
        if ( ! verifyChainContainsMonitoringListener(cr) ) {
            throw new IllegalStateException( "It was expected to find the memoery usage listener but it wasn't found" );
        }
        
    }

    private boolean verifyChainContainsMonitoringListener(ComponentRegistry cr) {
        InterceptorChain interceptorChain = cr.getComponent(InterceptorChain.class);
        return interceptorChain.containsInterceptorType(MemoryUsageListener.class, true);
    }

   
    @Override
    public void cacheStopped(ComponentRegistry cr, String cacheName) {
        Configuration cfg = cr.getComponent(Configuration.class);
        ConfigurationBuilder builder = new ConfigurationBuilder();
        CustomInterceptorsConfigurationBuilder customInterceptorsBuilder = builder.customInterceptors();

        //unregister cache level listener
        Cache<?, ?> cache = cr.getComponent(Cache.class);
        InterceptorChain interceptorChain = cr.getComponent(InterceptorChain.class);
        List<CommandInterceptor> mListener = interceptorChain.getInterceptorsWithClass(MemoryUsageListener.class);
        if (mListener != null && !mListener.isEmpty()) {
            MemoryUsageListener mInterceptor = (MemoryUsageListener)mListener.get(0);
            cache.removeListener(mInterceptor);
        } else {
            log.info("Error removing MemoryUsageInterceptor as listener");
        }
        log.info("Done emoving MemoryUsageInterceptor as listener");
        
        for (InterceptorConfiguration interceptorConfig : cfg.customInterceptors().interceptors()) {
            if (!(interceptorConfig.interceptor() instanceof MemoryUsageListener)) {
                //implicitly remove MemoryUsageListener from configuration
                customInterceptorsBuilder.addInterceptor().read(interceptorConfig); 
            } else {
                log.info("Unregistering MemoryUsageInterceptor");
            }
        }

        cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
    }


}
