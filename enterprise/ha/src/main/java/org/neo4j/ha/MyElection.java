package org.neo4j.ha;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.hazelcast.core.Member;

import org.neo4j.cluster.InstanceId;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;

import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.PENDING;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.TO_MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.TO_SLAVE;

public class MyElection
{

    private List<OutcomeListener> listeners = new ArrayList<>(  );

    public void triggerElection( Set<Member> members )
    {
        SortedSet<Member> sortedMembers = new TreeSet<Member>( new Comparator<Member>()
        {
            @Override
            public int compare( Member o1, Member o2 )
            {
                return o1.getIntAttribute( "server_id" ).compareTo( o2.getIntAttribute( "server_id" ) );
            }
        } );
        sortedMembers.addAll( members );


        if(sortedMembers.isEmpty()) {
            System.out.println( "**** No members :(; aborting election." );
            return;
        }

        if ( !sortedMembers.first().localMember() )
        {
            System.out.println( "**** Not the elector; aborting election." );
            return;
        }

        System.out.println( "**** ELECTION TIME!1!!1! Start the swing-ometer " + members );

        Iterator<Member> iterator = sortedMembers.iterator();
        Member master = iterator.next();
        notify( master, TO_MASTER );
        while ( iterator.hasNext() )
        {
            Member slave = iterator.next();
            notify( slave, TO_SLAVE );
        }
    }

    private void notify( Member member, HighAvailabilityMemberState newState )
    {
        HighAvailabilityMemberState oldState = PENDING;

        InstanceId instanceId = new InstanceId( member.getIntAttribute( "server_id" ) );
        System.out.println( "instanceId = " + instanceId + " # listeners (" + listeners.size() + ")" );
        final HighAvailabilityMemberChangeEvent event =
                new HighAvailabilityMemberChangeEvent( oldState, newState, instanceId, null );
        Listeners.notifyListeners( listeners,
                new Listeners.Notification<OutcomeListener>()
                {
                    @Override
                    public void notify( OutcomeListener listener )
                    {
                        System.out.println("&&&&& notifying listeners about master election");
                        listener.elected( event );
                    }

                } );
    }

    public void addOutcomeListener( OutcomeListener outcomeListener )
    {
        System.out.println(">>> OutcomeListener added");
        listeners.add(outcomeListener);
    }

    public interface OutcomeListener
    {
        void elected( HighAvailabilityMemberChangeEvent event );
    }
}
