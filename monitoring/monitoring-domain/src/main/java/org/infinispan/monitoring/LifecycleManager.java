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


import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.CustomInterceptorsConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.InterceptorConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.CacheMgmtInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.monitoring.interceptor.MemoryUsageInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Lifecycle of the Monitoring module: append MemoryUsageInterceptor to intercepter 
 * chain at cache starts and remove it at cache stops. 
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
        if (cfg.jmxStatistics().enabled()) {
            log.info("Registering Monitoring interceptor");
            createMonitoringInterceptorIfNeeded(cr, cfg);
        }
    }

    private void createMonitoringInterceptorIfNeeded(ComponentRegistry cr, Configuration cfg) {
        MemoryUsageInterceptor memoryInterceptor = cr.getComponent(MemoryUsageInterceptor.class);
        if (memoryInterceptor == null) {
            memoryInterceptor = new MemoryUsageInterceptor();
            // Interceptor registration not needed, core configuration handling
            // already does it for all custom interceptors - UNLESS the InterceptorChain already exists in the component registry!            
            InterceptorChain ic = cr.getComponent(InterceptorChain.class);
            
            if (ic != null && cr != null) {
                cr.registerComponent(memoryInterceptor, MemoryUsageInterceptor.class);
            }
            
            ConfigurationBuilder builder = new ConfigurationBuilder().read(cfg);
            InterceptorConfigurationBuilder interceptorBuilder = builder.customInterceptors().addInterceptor();
            interceptorBuilder.interceptor(memoryInterceptor);

            if (ic != null) {
                ic.addInterceptorAfter(memoryInterceptor, CacheMgmtInterceptor.class);
            }
            interceptorBuilder.after(CacheMgmtInterceptor.class);
            cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
        }
    }


    @Override
    public void cacheStarted(ComponentRegistry cr, String cacheName) {
        Configuration configuration = cr.getComponent(Configuration.class);
        boolean jmsStatEnabled = configuration.jmxStatistics().enabled();
        if ( ! jmsStatEnabled ) {
            if ( verifyChainContainsMonitoringInterceptor(cr) ) {
                throw new IllegalStateException( "It was NOT expected to find the Monitoring interceptor registered in the InterceptorChain as JMX statistics was disabled, but it was found" );
            }
            return;
        }
        if ( ! verifyChainContainsMonitoringInterceptor(cr) ) {
            throw new IllegalStateException( "It was expected to find the Monitoring interceptor registered in the InterceptorChain but it wasn't found" );
        }
    }

    private boolean verifyChainContainsMonitoringInterceptor(ComponentRegistry cr) {
        InterceptorChain interceptorChain = cr.getComponent(InterceptorChain.class);
        return interceptorChain.containsInterceptorType(MemoryUsageInterceptor.class, true);
    }

   
    @Override
    public void cacheStopped(ComponentRegistry cr, String cacheName) {
        Configuration cfg = cr.getComponent(Configuration.class);
        ConfigurationBuilder builder = new ConfigurationBuilder();
        CustomInterceptorsConfigurationBuilder customInterceptorsBuilder = builder.customInterceptors();

        for (InterceptorConfiguration interceptorConfig : cfg.customInterceptors().interceptors()) {
            if (!(interceptorConfig.interceptor() instanceof MemoryUsageInterceptor)) {
                //implicitly remove MemoryUsageInterceptor from configuration
                customInterceptorsBuilder.addInterceptor().read(interceptorConfig); 
            } else {
                log.info("Unregistering Monitoring interceptor");
            }
        }

        cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
    }


}
