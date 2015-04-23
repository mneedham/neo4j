/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha.factory;

import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.List;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class Neo4jHazelcastInstance extends LifecycleAdapter
{
    private Config config;
    private HazelcastInstance hazelcastInstance;

    public Neo4jHazelcastInstance( Config config )
    {
        this.config = config;
    }

    @Override
    public void init() throws Throwable
    {
        hazelcastInstance = createHazelcastInstance();
    }



    @Override
    public void start() throws Throwable
    {
        if(hazelcastInstance == null) {
            hazelcastInstance = createHazelcastInstance();
        }
    }

    @Override
    public void stop() throws Throwable
    {
        hazelcastInstance.shutdown();
        hazelcastInstance = null;
    }

    public HazelcastInstance getHazelcastInstance()
    {
        return hazelcastInstance;
    }

    private HazelcastInstance createHazelcastInstance(  )
    {
        JoinConfig joinConfig = new JoinConfig();
        joinConfig.getMulticastConfig().setEnabled( false );
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
        tcpIpConfig.setEnabled( true );

        List<HostnamePort> hostnamePorts = config.get( ClusterSettings.initial_hosts );
        for ( HostnamePort hostnamePort : hostnamePorts )
        {
            tcpIpConfig.addMember( hostnamePort.getHost() + ":" + hostnamePort.getPort() );
        }

        NetworkConfig networkConfig = new NetworkConfig();
        networkConfig.setPort( config.get( ClusterSettings.cluster_server ).getPort() );
        networkConfig.setJoin( joinConfig );
//        networkConfig.getInterfaces().setInterfaces( Arrays.asList( "127.0.0.1" ) ).setEnabled( true );
//        networkConfig.getInterfaces().setInterfaces( Arrays.asList( "192.168.1.12" ) ).setEnabled( true );
//        networkConfig.getInterfaces()
//                .setInterfaces( Arrays.asList(config.get( ClusterSettings.cluster_server ).getHost()) )
//                .setEnabled( true );

        String instanceName = String.valueOf( config.get( ClusterSettings.server_id ).toIntegerIndex() );
        System.out.println( "instanceName = " + instanceName );
        com.hazelcast.config.Config c = new com.hazelcast.config.Config( instanceName );
        c.setProperty( "hazelcast.initial.min.cluster.size", "2" );
        c.setNetworkConfig( networkConfig );

        System.out.println("creating HC instance with " + c.getInstanceName());
        return Hazelcast.newHazelcastInstance( c );
    }

    public String getName()
    {
        return hazelcastInstance.getName();
    }
}
