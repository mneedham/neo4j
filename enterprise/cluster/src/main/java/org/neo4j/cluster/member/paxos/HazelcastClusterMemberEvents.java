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
package org.neo4j.cluster.member.paxos;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.instance.MemberImpl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.protocol.snapshot.SnapshotProvider;
import org.neo4j.function.Predicate;
import org.neo4j.helpers.Function2;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha.factory.Neo4jHazelcastInstance;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.helpers.Predicates.in;
import static org.neo4j.helpers.Predicates.not;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.toList;

/**
 * Hazelcast based implementation of {@link org.neo4j.cluster.member.ClusterMemberEvents}
 */
public class HazelcastClusterMemberEvents implements ClusterMemberEvents, Lifecycle
{
    private final MemberMessageListener topicListener;
    private Log log;
    protected Iterable<ClusterMemberListener> listeners = Listeners.newListeners();
    private ExecutorService executor;
    private Neo4jHazelcastInstance instance;
    private final NamedThreadFactory.Monitor namedThreadFactoryMonitor;
    private String registrationId;
    private final MembershipListener membershipListener;
    private String membershipListenerRegistrationId;

    public HazelcastClusterMemberEvents( Neo4jHazelcastInstance instance, LogProvider logProvider,
            NamedThreadFactory.Monitor namedThreadFactoryMonitor )
    {
        this.instance = instance;
        this.namedThreadFactoryMonitor = namedThreadFactoryMonitor;
        this.log = logProvider.getLog( getClass() );

        topicListener = new MemberMessageListener();
        membershipListener = new OurMembershipListener();
    }

    @Override
    public void addClusterMemberListener( ClusterMemberListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    @Override
    public void removeClusterMemberListener( ClusterMemberListener listener )
    {
        listeners = Listeners.removeListener( listener, listeners );
    }

    @Override
    public void init()
            throws Throwable
    {
        ITopic<MemberAvailabilityMessage> topic = instance.getHazelcastInstance().getTopic( "cluster-membership" );
        registrationId = topic.addMessageListener( topicListener );

        executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("Paxos event notification", namedThreadFactoryMonitor));
    }

    @Override
    public void start()
            throws Throwable
    {
        membershipListenerRegistrationId = instance.getHazelcastInstance().getCluster().addMembershipListener( membershipListener );
    }

    @Override
    public void stop()
            throws Throwable
    {
        instance.getHazelcastInstance().getCluster().removeMembershipListener( membershipListenerRegistrationId );
    }

    @Override
    public void shutdown()
            throws Throwable
    {
        if ( executor != null )
        {
            executor.shutdown();
            executor = null;
        }
        if(instance != null && instance.getHazelcastInstance() != null) {
            instance.getHazelcastInstance().getTopic( "cluster-membership" ).removeMessageListener( registrationId );
        }

    }

    private class MemberMessageListener implements MessageListener<MemberAvailabilityMessage>
    {
        @Override
        public void onMessage( Message<MemberAvailabilityMessage> message )
        {
            try
            {
                final Object value = message.getMessageObject();
                if ( value instanceof MemberIsAvailable )
                {
                    final MemberIsAvailable memberIsAvailable = (MemberIsAvailable) value;

                    Listeners.notifyListeners( listeners, new Listeners.Notification<ClusterMemberListener>()
                    {
                        @Override
                        public void notify( ClusterMemberListener listener )
                        {
                            listener.memberIsAvailable( memberIsAvailable.getRole(), memberIsAvailable.getInstanceId(),
                                    memberIsAvailable.getRoleUri(), memberIsAvailable.getStoreId() );
                        }
                    } );
                }
                else if ( value instanceof MemberIsUnavailable )
                {
                    final MemberIsUnavailable memberIsUnavailable = (MemberIsUnavailable) value;

                    Listeners.notifyListeners( listeners, new Listeners.Notification<ClusterMemberListener>()
                    {
                        @Override
                        public void notify( ClusterMemberListener listener )
                        {
                            listener.memberIsUnavailable( memberIsUnavailable.getRole(),
                                    memberIsUnavailable.getInstanceId() );
                        }
                    } );
                }
            }
            catch ( Throwable t )
            {
                log.error( "Could not handle cluster member available message", t );
            }
        }
    }

    private class OurMembershipListener implements MembershipListener, InitialMembershipListener
    {
        @Override
        public void memberAdded( final MembershipEvent membershipEvent )
        {
            Listeners.notifyListeners( listeners, executor, new Listeners.Notification<ClusterMemberListener>()
            {
                @Override
                public void notify( ClusterMemberListener listener )
                {
                    listener.memberIsAlive( new InstanceId(((MemberImpl) membershipEvent.getMember()).getId()) );
                }
            } );
        }

        @Override
        public void memberRemoved( final MembershipEvent membershipEvent )
        {
            Listeners.notifyListeners( listeners, executor, new Listeners.Notification<ClusterMemberListener>()
            {
                @Override
                public void notify( ClusterMemberListener listener )
                {
                    listener.memberIsFailed( new InstanceId( ((MemberImpl) membershipEvent.getMember()).getId() ) );
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
            Listeners.notifyListeners( listeners, executor, new Listeners.Notification<ClusterMemberListener>()
            {
                @Override
                public void notify( ClusterMemberListener listener )
                {
                    for ( Member member : event.getMembers() )
                    {
                        System.out.println("****member****" + member + " -> " + member.getClass());
                        listener.memberIsAlive( new InstanceId( ((MemberImpl) member).getId() ) );
                    }

                }
            } );
        }
    }
}
