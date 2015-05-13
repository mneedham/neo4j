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
package org.neo4j.coreedge;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class MemberAvailabilityWhiteboard extends LifecycleAdapter implements ClusterMemberAvailability,
        ClusterMemberEvents

{
    public static final URI FAKE_URI = URI.create( "cluster://localhost:1000" );
    private final HazelcastLifecycle hazelcastLifecycle;
    private List<ClusterMemberListener> listeners = new ArrayList<>();
    public static final String MAP_AVAILABILITY = "AVAILABILITY";
    private IMap<Integer, ClusterMemberAvailabilityState> map;
    private ITopic<ClusterMemberAvailabilityState> topic;

    public MemberAvailabilityWhiteboard( final HazelcastLifecycle hazelcastLifecycle )
    {
        this.hazelcastLifecycle = hazelcastLifecycle;

        hazelcastLifecycle.addStartupListener( new HazelcastLifecycle.StartupListener()
        {
            @Override public void hazelcastStarted( HazelcastInstance hazelcastInstance )
            {
                topic = hazelcastInstance.getTopic( MAP_AVAILABILITY );
                topic.addMessageListener( new MessageListener<ClusterMemberAvailabilityState>()
                {
                    @Override
                    public void onMessage( Message<ClusterMemberAvailabilityState> message )
                    {
                        ClusterMemberAvailabilityState state = message.getMessageObject();

                        notifyAvailability( state );
                    }
                } );

                map = hazelcastInstance.getMap( MAP_AVAILABILITY );

                for ( ClusterMemberAvailabilityState value : map.values() )
                {
                    notifyAvailability( value );
                }
            }
        } );

    }

    private void notifyAvailability( final ClusterMemberAvailabilityState availabilityState )
    {
        if ( availabilityState != null )
        {
            Listeners.notifyListeners(listeners, new Listeners.Notification<ClusterMemberListener>()
            {
                @Override public void notify( ClusterMemberListener listener )
                {
                    if ( availabilityState.isAvailable() )
                    {
                        listener.memberIsAvailable(
                                availabilityState.getRole(), availabilityState.getInstanceId(),
                                availabilityState.getAtUri(), availabilityState.getStoreId() );
                    }
                    else
                    {
                        listener.memberIsUnavailable(
                                availabilityState.getRole(), availabilityState.getInstanceId() );

                    }
                }
            });

        }
    }

    @Override
    public void start() throws Throwable
    {
    }

    @Override
    public void addClusterMemberListener( ClusterMemberListener listener )
    {
        listeners.add( listener );
    }

    @Override
    public void removeClusterMemberListener( ClusterMemberListener listener )
    {
        listeners.remove( listener );
    }

    @Override
    public void memberIsAvailable( String role, URI roleUri, StoreId storeId )
    {
        InstanceId instanceId = hazelcastLifecycle.myId();
        ClusterMemberAvailabilityState state =
                new ClusterMemberAvailabilityState( instanceId, role, roleUri, storeId, true );
        map.put( instanceId.toIntegerIndex(), state );
        topic.publish( state );
    }

    @Override
    public void memberIsUnavailable( String role )
    {
        InstanceId instanceId = hazelcastLifecycle.myId();
        ClusterMemberAvailabilityState state =
                new ClusterMemberAvailabilityState( instanceId, role, null, null, false );
        map.put( instanceId.toIntegerIndex(),
                state );
        topic.publish( state );
    }
}
