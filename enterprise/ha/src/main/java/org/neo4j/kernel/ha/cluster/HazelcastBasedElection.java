package org.neo4j.kernel.ha.cluster;

import com.hazelcast.collection.CollectionService;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.kernel.ha.factory.Neo4jHazelcastInstance;

public class HazelcastBasedElection implements HighAvailability
{
    private final Cluster cluster;
    private LifecycleService lifecycleService;
    private List<HighAvailabilityMemberListener> listeners = new ArrayList<>();

    public HazelcastBasedElection( HazelcastInstance instance, List<HighAvailabilityMemberListener> listeners )
    {
        this.cluster = instance.getCluster();
        this.lifecycleService = instance.getLifecycleService();
        this.listeners = listeners;

        cluster.addMembershipListener( new MyMembershipListener() );

        lifecycleService.addLifecycleListener( new LifecycleListener()
        {
            @Override
            public void stateChanged( LifecycleEvent event )
            {
                System.out.println("LifecycleEvent received: " + event);
            }
        } );
    }

    void runElection()
    {
        List<Member> members = new ArrayList<>( cluster.getMembers() );
        Collections.sort( members, new Comparator<Member>()
        {
            @Override
            public int compare( Member o1, Member o2 )
            {
                return o1.getUuid().compareTo( o2.getUuid() );
            }
        } );
        Iterator<Member> iterator = members.iterator();
        Member master = iterator.next();
        while ( iterator.hasNext() )
        {
            Member slave = iterator.next();
        }
        for ( HighAvailabilityMemberListener listener : listeners )
        {
            listener.masterIsElected(
                    new HighAvailabilityMemberChangeEvent(
                            HighAvailabilityMemberState.TO_MASTER,
                            HighAvailabilityMemberState.MASTER,
                            null,
                            null ) );
        }
    }

    @Override
    public void addHighAvailabilityMemberListener( HighAvailabilityMemberListener listener )
    {
        listeners.add( listener );
    }

    @Override
    public void removeHighAvailabilityMemberListener( HighAvailabilityMemberListener listener )
    {
        listeners.remove( listener );
    }

    private class MyMembershipListener implements MembershipListener, InitialMembershipListener
    {
        @Override
        public void memberAdded( MembershipEvent membershipEvent )
        {
            System.out.println( "Member added: " + membershipEvent );
            runElection();
        }

        @Override
        public void memberRemoved( MembershipEvent membershipEvent )
        {
            System.out.println( "Member removed: " + membershipEvent );
            runElection();
        }

        @Override
        public void memberAttributeChanged( MemberAttributeEvent memberAttributeEvent )
        {
            System.out.println( "Member attribute changed: " + memberAttributeEvent );
            runElection();
        }

        @Override
        public void init( InitialMembershipEvent event )
        {
            System.out.println( "Init: " + event );
            runElection();
        }
    }
}
