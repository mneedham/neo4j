/*
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
package org.neo4j.cluster.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

import org.neo4j.cluster.InstanceId;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class HazelcastLifecycle extends LifecycleAdapter
{
    public static final String CLUSTER_SERVER = "cluster_server";
    public static final String SERVER_ID = "server_id";

    private ClusterClient.Configuration config;
    private HazelcastInstance hazelcastInstance;

    private List<StartupListener> startupListeners = new ArrayList<>();

    public HazelcastLifecycle( ClusterClient.Configuration config )
    {
        this.config = config;
    }

    public static InstanceId instanceIdFor( Member member )
    {
        return new InstanceId( member.getIntAttribute( "server_id" ) );
    }

    @Override
    public void start() throws Throwable
    {
        hazelcastInstance = createHazelcastInstance();

        Listeners.notifyListeners(startupListeners, new Listeners.Notification<StartupListener>()
        {
            @Override public void notify( StartupListener listener )
            {
                listener.hazelcastStarted( hazelcastInstance );
            }
        });
    }

    @Override
    public void stop() throws Throwable
    {
        hazelcastInstance.shutdown();
    }

    private HazelcastInstance createHazelcastInstance()
    {
        JoinConfig joinConfig = new JoinConfig();
        joinConfig.getMulticastConfig().setEnabled( false );
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
        tcpIpConfig.setEnabled( true );

        for ( HostnamePort hostnamePort : config.getInitialHosts() )
        {
            tcpIpConfig.addMember( hostnamePort.getHost() + ":" + (hostnamePort.getPort()) );
        }

        NetworkConfig networkConfig = new NetworkConfig();
        networkConfig.setPort( config.getAddress().getPort() );
        networkConfig.setJoin( joinConfig );
        String instanceName = String.valueOf( config.getServerId().toIntegerIndex() );
        System.out.println( "instanceName = " + instanceName );
        com.hazelcast.config.Config c = new com.hazelcast.config.Config( instanceName );
        c.setProperty( "hazelcast.initial.min.cluster.size", "2" );
        c.setNetworkConfig( networkConfig );

        MemberAttributeConfig memberAttributeConfig = new MemberAttributeConfig();
        memberAttributeConfig.setIntAttribute( SERVER_ID, config.getServerId().toIntegerIndex() );
        String clusterServer = "cluster://" + config.getAddress().getHost() + ":" + config.getAddress().getPort();
        memberAttributeConfig.setStringAttribute( CLUSTER_SERVER, clusterServer );
        c.setMemberAttributeConfig( memberAttributeConfig );

        return Hazelcast.newHazelcastInstance( c );
    }

    public interface StartupListener
    {
        void hazelcastStarted( HazelcastInstance hazelcastInstance );
    }

    public void addStartupListener( StartupListener startupListener )
    {
        startupListeners.add( startupListener );
    }

    public Set<Member> members()
    {
        return hazelcastInstance.getCluster().getMembers();
    }

    public InstanceId myId()
    {
        return instanceIdFor( hazelcastInstance.getCluster().getLocalMember() );
    }
}
