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

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.com.NetworkInstance;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.DevNullLoggingService;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * TODO
 */
public class NetworkInstanceSendAndReceiveTest
{
    public enum TestMessage
            implements MessageType
    {
        helloWorld;
    }

    @Test
    public void canConnectThroughAllLoopbackInterfacesOnPort5001IfClusterServerNotExplicitlySet() throws IOException {
        CountDownLatch latch = new CountDownLatch( 1 );
        LifeSupport life = new LifeSupport();
        Server server1 = new Server( latch, MapUtil.stringMap( ClusterSettings.server_id.name(), "1",
                ClusterSettings.initial_hosts.name(), "localhost:1235,localhost:5001" ) );
        life.add( server1 );
        life.start();

        NetworkInterface localLoopback = getLocalLoopbackInterface(Collections.list(NetworkInterface.getNetworkInterfaces()));
        ArrayList<InetAddress> localLoopBackAddresses = Collections.list(localLoopback.getInetAddresses());
        assertTrue(localLoopBackAddresses.size() > 0);
        for (InetAddress inetAddress : localLoopBackAddresses) {
            boolean canConnect = canConnectTo5001(inetAddress);
            assertTrue(String.format("Failed to create socket connection to *.5001 using %s [%s]", localLoopback.getDisplayName(), inetAddress), canConnect);
        }
    }

    @Test
    public void canConnectThroughAtLeastOneExternalInterfaceOnPort5001IfClusterServerNotExplicitlySet() throws IOException {
        CountDownLatch latch = new CountDownLatch( 1 );
        LifeSupport life = new LifeSupport();
        Server server1 = new Server( latch, MapUtil.stringMap( ClusterSettings.server_id.name(), "1",
                ClusterSettings.initial_hosts.name(), "localhost:1235,localhost:5001" ) );
        life.add( server1 );
        life.start();

        List<NetworkInterface> nonLoopbackInterfaces = getNonLoopbackInterfaces(Collections.list(NetworkInterface.getNetworkInterfaces()));
        assertTrue(nonLoopbackInterfaces.size() > 0);

        boolean canConnectThroughAtLeastOne = false;
        for (NetworkInterface networkInterface : nonLoopbackInterfaces) {
            ArrayList<InetAddress> nonLoopBackAddresses = Collections.list(networkInterface.getInetAddresses());
            for (InetAddress inetAddress : nonLoopBackAddresses) {
                boolean canConnect = canConnectTo5001(inetAddress);
                System.out.println("canConnect = " + canConnect + " " + inetAddress);
                canConnectThroughAtLeastOne |=  canConnect;
            }
        }
        assertTrue(canConnectThroughAtLeastOne);
    }

    private List<NetworkInterface> getNonLoopbackInterfaces(ArrayList<NetworkInterface> networkInterfaces) throws SocketException {
        List<NetworkInterface> nonLoopback = new ArrayList<NetworkInterface>();
        for (NetworkInterface networkInterface : networkInterfaces) {
            if(!networkInterface.isLoopback()) {
                nonLoopback.add(networkInterface);
            }
        }
        return nonLoopback;
    }

    private boolean canConnectTo5001(InetAddress inetAddress) throws IOException {
        try {
            new Socket(inetAddress, 5001);
            return true;
        } catch(ConnectException ex) {
            return false;
        }
    }

    private NetworkInterface getLocalLoopbackInterface(ArrayList<NetworkInterface> networkInterfaces) throws SocketException {
        for (NetworkInterface networkInterface : networkInterfaces) {
            if(networkInterface.isLoopback()) {
                return networkInterface;
            }
        }
        throw new RuntimeException("No loopback interface");
    }

    @Test
    public void shouldSendAMessageFromAClientWhichIsReceivedByAServer() throws Exception
    {

        // given

        CountDownLatch latch = new CountDownLatch( 1 );

        LifeSupport life = new LifeSupport();

        Server server1 = new Server( latch, MapUtil.stringMap( ClusterSettings.cluster_server.name(),
                "localhost:1234", ClusterSettings.server_id.name(), "1",
                ClusterSettings.initial_hosts.name(), "localhost:1234,localhost:1235" ) );

        life.add( server1 );

        Server server2 = new Server( latch, MapUtil.stringMap( ClusterSettings.cluster_server.name(), "localhost:1235",
                ClusterSettings.server_id.name(), "2",
                ClusterSettings.initial_hosts.name(), "localhost:1234,localhost:1235" ) );

        life.add( server2 );

        life.start();

        // when

        server1.process( Message.to( TestMessage.helloWorld, URI.create( "neo4j://127.0.0.1:1235" ), "Hello World" ) );

        // then

        latch.await( 2, TimeUnit.SECONDS );

        assertTrue( server1.processedMessage() );
        assertTrue( server2.processedMessage() );

        life.shutdown();
    }

    private static class Server
            implements Lifecycle, MessageProcessor
    {
        protected NetworkInstance networkInstance;

        private final LifeSupport life = new LifeSupport();
        private boolean processedMessage = false;

        private Server( final CountDownLatch latch, final Map<String, String> config )
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
}
