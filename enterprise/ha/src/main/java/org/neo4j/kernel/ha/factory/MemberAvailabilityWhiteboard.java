package org.neo4j.kernel.ha.factory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.HazelcastLifecycle;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.member.ClusterMemberAvailabilityState;
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

                        System.out.println( "()()() notifying memberIsAvailable " + state );
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
        System.out.println( "*MemberAvailabilityWhiteBoard#notifying " + listeners.size() + " listeners" );
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
        System.out.println( "^^^^ Added cluster member listener" );
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
