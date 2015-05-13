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

import java.util.ArrayList;
import java.util.List;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;


public class ElectionOutcomeWhiteboard extends LifecycleAdapter implements ClusterMemberEvents
{
    private List<ClusterMemberListener> listeners = new ArrayList<>();
    public static final String MAP_ROLES = "ROLES";

    public ElectionOutcomeWhiteboard( final HazelcastLifecycle hazelcastLifecycle, final Election election )
    {
        hazelcastLifecycle.addStartupListener( new HazelcastLifecycle.StartupListener()
        {
            @Override
            public void hazelcastStarted( final HazelcastInstance hazelcastInstance )
            {
                final ITopic<InstanceId> topic = hazelcastInstance.getTopic( MAP_ROLES );
                topic.addMessageListener( new MessageListener<InstanceId>()
                {
                    @Override
                    public void onMessage( Message<InstanceId> message )
                    {
                        InstanceId instanceId = message.getMessageObject();
                        notifyElectionOutcome( instanceId );
                    }
                } );

                final IAtomicReference<InstanceId> coordinator = hazelcastInstance.getAtomicReference( "coordinator" );
                notifyElectionOutcome( coordinator.get() );

                election.addOutcomeListener( new ClusterMemberListener.Adapter()
                {
                    @Override
                    public void coordinatorIsElected( InstanceId coordinatorId )
                    {
                        coordinator.set( coordinatorId );
                        topic.publish( coordinatorId );
                    }

                } );
            }
        } );
    }

    private void notifyElectionOutcome( final InstanceId instanceId )
    {
        if ( instanceId != null )
        {
            Listeners.notifyListeners(listeners, new Listeners.Notification<ClusterMemberListener>()
            {
                @Override public void notify( ClusterMemberListener listener )
                {
                    listener.coordinatorIsElected( instanceId );
                }
            });
        }
    }

    @Override
    public void start() throws Throwable
    {
    }

    public void addClusterMemberListener( ClusterMemberListener toAdd )
    {
        listeners.add( toAdd );
    }

    @Override public void removeClusterMemberListener( ClusterMemberListener listener )
    {

    }
}
