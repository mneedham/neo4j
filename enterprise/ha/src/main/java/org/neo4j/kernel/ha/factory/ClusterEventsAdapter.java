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
package org.neo4j.kernel.ha.factory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;

import org.neo4j.cluster.client.HazelcastLifecycle;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.helpers.Listeners;
import org.neo4j.logging.LogProvider;

import static org.neo4j.cluster.client.HazelcastLifecycle.CLUSTER_SERVER;

public class ClusterEventsAdapter implements Cluster
{
    private final List<ClusterListener> listeners = new ArrayList<>();

    public ClusterEventsAdapter( HazelcastLifecycle hazelcast, final LogProvider logProvider )
    {
        hazelcast.addStartupListener( new HazelcastLifecycle.StartupListener()
        {
            @Override
            public void hazelcastStarted( HazelcastInstance hazelcastInstance )
            {
                hazelcastInstance.getCluster().addMembershipListener( new InitialMembershipListener()
                {
                    @Override
                    public void memberAdded( final MembershipEvent membershipEvent )
                    {
                        System.out.println("Received memberAdded " + membershipEvent);
                        Listeners.notifyListeners(listeners, new Listeners.Notification<ClusterListener>()
                        {
                            @Override public void notify( ClusterListener listener )
                            {
                                Member member = membershipEvent.getMember();
                                listener.joinedCluster( HazelcastLifecycle.instanceIdFor( member ),
                                        URI.create( member.getStringAttribute( CLUSTER_SERVER ) ) );
                            }
                        });
                    }

                    @Override
                    public void memberRemoved( final MembershipEvent membershipEvent )
                    {
                        Listeners.notifyListeners( listeners, new Listeners.Notification<ClusterListener>()
                        {
                            @Override public void notify( ClusterListener listener )
                            {
                                Member member = membershipEvent.getMember();
                                listener.leftCluster( HazelcastLifecycle.instanceIdFor( member ),
                                        URI.create( member.getStringAttribute( CLUSTER_SERVER ) ) );
                            }
                        } );
                    }

                    @Override
                    public void memberAttributeChanged( MemberAttributeEvent memberAttributeEvent )
                    {

                    }

                    @Override
                    public void init( final InitialMembershipEvent event )
                    {
                        final ClusterConfiguration clusterConfiguration = new ClusterConfiguration( "neo4j.ha", logProvider );

                        for ( Member member : event.getMembers() )
                        {
                            clusterConfiguration.joined( HazelcastLifecycle.instanceIdFor( member ),
                                    URI.create( member.getStringAttribute( CLUSTER_SERVER ) ) );
                        }

                        Listeners.notifyListeners( listeners, new Listeners.Notification<ClusterListener>()
                        {
                            @Override public void notify( ClusterListener listener )
                            {

                                listener.enteredCluster( clusterConfiguration );
                            }
                        } );
                    }
                } );
            }
        } );
    }

    @Override
    public void create( String clusterName )
    {
        throw new UnsupportedOperationException("This adaptor is for subscribing to events, not for anything else.");
    }

    @Override
    public Future<ClusterConfiguration> join( String clusterName, URI... otherServerUrls )
    {
        return null;
    }

    @Override
    public void leave()
    {

    }

    @Override
    public void addClusterListener( ClusterListener listener )
    {
        listeners.add(listener);
    }

    @Override
    public void removeClusterListener( ClusterListener listener )
    {
        listeners.remove(listener);
    }
}
