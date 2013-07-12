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

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.lifecycle.LifeSupport;

import static org.junit.Assert.assertTrue;

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
    public void shouldSendAMessageFromAClientToAServerWhenTheServerDoesNotExplicitlySetItsClusterServer() throws Exception {
        // given

        CountDownLatch latch = new CountDownLatch( 1 );

        LifeSupport life = new LifeSupport();

        Server server1 = new Server( latch, MapUtil.stringMap( ClusterSettings.cluster_server.name(),
                "localhost:1234", ClusterSettings.server_id.name(), "1",
                ClusterSettings.initial_hosts.name(), "localhost:1234,localhost:1235" ) );

        life.add( server1 );

        Server serverWithoutExplicitServerName = new Server( latch, MapUtil.stringMap( ClusterSettings.server_id.name(), "2",
                ClusterSettings.initial_hosts.name(), "localhost:1234,localhost:1235" ) );

        life.add( serverWithoutExplicitServerName );

        life.start();

        // when

        server1.process( Message.to( TestMessage.helloWorld, URI.create( "neo4j://127.0.0.1:5001" ), "Hello World" ) );

        // then

        latch.await( 2, TimeUnit.SECONDS );

        assertTrue( server1.processedMessage() );
        assertTrue(serverWithoutExplicitServerName.processedMessage());

        life.shutdown();
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

}
