package org.neo4j.ha;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.hazelcast.core.Member;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.helpers.Listeners;

import static org.neo4j.cluster.client.HazelcastLifecycle.instanceIdFor;

public class MyElection
{
    private List<ClusterMemberListener> listeners = new ArrayList<>();

    public void triggerElection( Set<Member> members )
    {
        SortedSet<Member> sortedMembers = new TreeSet<Member>( new Comparator<Member>()
        {
            @Override
            public int compare( Member o1, Member o2 )
            {
                return instanceIdFor( o1 ).compareTo( instanceIdFor( o2 ) );
            }
        } );
        sortedMembers.addAll( members );


        if ( sortedMembers.isEmpty() )
        {
            System.out.println( "**** No members :(; aborting election." );
            return;
        }

        if ( !sortedMembers.first().localMember() )
        {
            System.out.println( "**** Not the elector; aborting election." );
            return;
        }

        Member master = sortedMembers.first();
        final InstanceId instanceId = instanceIdFor(master);

        Listeners.notifyListeners( listeners,
                new Listeners.Notification<ClusterMemberListener>()
                {
                    @Override
                    public void notify( ClusterMemberListener listener )
                    {
                        listener.coordinatorIsElected( instanceId );
                    }
                } );
    }

    public void addOutcomeListener( ClusterMemberListener memberListener )
    {
        listeners.add( memberListener );
    }
}
