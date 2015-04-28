package org.neo4j.kernel.ha.factory;

import java.util.ArrayList;
import java.util.List;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;

import org.neo4j.cluster.client.HazelcastLifecycle;
import org.neo4j.ha.MyElection;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.TO_MASTER;


public class ElectionOutcomeWhiteboard extends LifecycleAdapter
{
    private final HazelcastLifecycle hazelcastLifecycle;
    private final MyElection election;
    private List<HighAvailabilityMemberListener> listeners = new ArrayList<>();
    public static final String MAP_ROLES = "ROLES";

    public ElectionOutcomeWhiteboard( final HazelcastLifecycle hazelcastLifecycle, MyElection election )
    {
        this.hazelcastLifecycle = hazelcastLifecycle;
        this.election = election;
        hazelcastLifecycle.addStartupListener( new HazelcastLifecycle.StartupListener()
        {
            @Override
            public void hazelcastStarted( final HazelcastInstance hazelcastInstance )
            {
                final IMap<Integer, HighAvailabilityMemberChangeEvent> map = hazelcastInstance.getMap( MAP_ROLES );
                map.addEntryListener( new EntryAdapter<Integer, HighAvailabilityMemberChangeEvent>()
                {
                    @Override
                    public void entryUpdated( EntryEvent<Integer, HighAvailabilityMemberChangeEvent> event )
                    {
                        if ( event.getKey().equals(hazelcastLifecycle.myId().toIntegerIndex()) )
                        {
                            notifyElectionOutcome( event.getValue() );
                        }
                    }

                    @Override
                    public void entryAdded( EntryEvent<Integer, HighAvailabilityMemberChangeEvent> event )
                    {
                        if ( event.getKey().equals(hazelcastLifecycle.myId().toIntegerIndex()) )
                        {
                            notifyElectionOutcome( event.getValue() );
                        }
                    }
                }, true );
                HighAvailabilityMemberChangeEvent availabilityState = map.get( hazelcastLifecycle.myId().toIntegerIndex() );

                notifyElectionOutcome( availabilityState );

                ElectionOutcomeWhiteboard.this.election.addOutcomeListener( new MyElection.OutcomeListener()
                {
                    @Override
                    public void elected( HighAvailabilityMemberChangeEvent event )
                    {
                        map.put( event.getInstanceId().toIntegerIndex(), event );
                    }
                } );
            }
        } );
    }

    private void notifyElectionOutcome( HighAvailabilityMemberChangeEvent state )
    {
        if ( state != null && state.getNewState().equals( TO_MASTER ) )
        {
            for ( HighAvailabilityMemberListener listener : listeners )
            {
                listener.masterIsElected( state );
            }
        }
    }

    @Override
    public void start() throws Throwable
    {
    }

    public void addHighAvailabilityMemberListener( HighAvailabilityMemberListener toAdd )
    {
        listeners.add( toAdd );
    }
}
