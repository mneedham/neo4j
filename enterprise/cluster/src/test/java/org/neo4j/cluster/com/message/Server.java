/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cluster.com.message;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.com.NetworkInstance;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.DevNullLoggingService;

import java.util.Map;
import java.util.concurrent.CountDownLatch;


class Server
        implements Lifecycle, MessageProcessor
{
    protected NetworkInstance networkInstance;

    private final LifeSupport life = new LifeSupport();
    private boolean processedMessage = false;

    Server(final CountDownLatch latch, final Map<String, String> config)
    {
        final Config conf = new Config( config, ClusterSettings.class );
        networkInstance = new NetworkInstance( new NetworkInstance.Configuration()
        {
            @Override
            public HostnamePort clusterServer()
            {
                return conf.get( ClusterSettings.cluster_server );
            }

            @Override
            public int defaultPort()
            {
                return 5001;
            }
        }, new DevNullLoggingService() );

        life.add( networkInstance );
        life.add( new LifecycleAdapter()
        {
            @Override
            public void start() throws Throwable
            {
                networkInstance.addMessageProcessor( new MessageProcessor()
                {
                    @Override
                    public boolean process( Message<? extends MessageType> message )
                    {
                        // server receives a message
                        latch.countDown();
                        processedMessage = true;
                        return true;
                    }
                } );
            }
        } );
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {

        life.start();
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
    }

    @Override
    public boolean process( Message<? extends MessageType> message )
    {
        // server sends a message
        this.processedMessage = true;
        return networkInstance.process( message );
    }

    public boolean processedMessage()
    {
        return this.processedMessage;
    }
}
