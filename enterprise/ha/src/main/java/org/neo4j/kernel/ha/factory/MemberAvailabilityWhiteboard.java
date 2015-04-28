package org.neo4j.kernel.ha.factory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.HazelcastLifecycle;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.member.ClusterMemberAvailabilityState;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
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

    public MemberAvailabilityWhiteboard( final HazelcastLifecycle hazelcastLifecycle )
    {
        this.hazelcastLifecycle = hazelcastLifecycle;

        hazelcastLifecycle.addStartupListener( new HazelcastLifecycle.StartupListener()
        {
            @Override public void hazelcastStarted( HazelcastInstance hazelcastInstance )
            {
                map = hazelcastInstance.getMap( MAP_AVAILABILITY );
                map.addEntryListener( new EntryAdapter<Integer, ClusterMemberAvailabilityState>()
                {
                    @Override
                    public void entryUpdated( EntryEvent<Integer, ClusterMemberAvailabilityState> event )
                    {
                        if ( event.getKey().equals(hazelcastLifecycle.myId().toIntegerIndex()) )
                        {
                            notifyAvailability( event.getValue() );
                        }
                    }

                    @Override
                    public void entryAdded( EntryEvent<Integer, ClusterMemberAvailabilityState> event )
                    {
                        if ( event.getKey().equals(hazelcastLifecycle.myId().toIntegerIndex()) )
                        {
                            notifyAvailability( event.getValue() );
                        }
                    }
                }, true );
                ClusterMemberAvailabilityState availabilityState =
                        map.get( hazelcastLifecycle.myId().toIntegerIndex() );
                notifyAvailability( availabilityState );
            }
        } );

    }

    private void notifyAvailability( ClusterMemberAvailabilityState availabilityState )
    {
        if ( availabilityState != null )
        {
            for ( ClusterMemberListener listener : listeners )
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
        }
    }

    @Override
    public void start() throws Throwable
    {
    }

    @Override
    public void addClusterMemberListener( ClusterMemberListener listener )
    {
        System.out.println("^^^^ Added cluster member listener");
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
        map.put( instanceId.toIntegerIndex(),
                new ClusterMemberAvailabilityState( instanceId, role, FAKE_URI, storeId, true ) );
    }

    @Override
    public void memberIsUnavailable( String role )
    {
        InstanceId instanceId = hazelcastLifecycle.myId();
        map.put( instanceId.toIntegerIndex(),
                new ClusterMemberAvailabilityState( instanceId, role, FAKE_URI, null, false ) );
    }
}
