package org.neo4j.coreedge;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;


public class ElectionInstigator
{
    private final HazelcastLifecycle hazelcast;
    private final Election election;

    public ElectionInstigator( HazelcastLifecycle hazelcast, Election election )
    {
        this.hazelcast = hazelcast;
        this.hazelcast.addStartupListener( new HazelcastLifecycle.StartupListener()
        {
            @Override
            public void hazelcastStarted( HazelcastInstance hazelcastInstance )
            {
                hazelcastInstance.getCluster().addMembershipListener( new InitialMembershipListener()
                {
                    private final Election election = ElectionInstigator.this.election;

                    @Override
                    public void memberAdded( final MembershipEvent membershipEvent )
                    {
                        election.triggerElection( membershipEvent.getMembers() );
                    }

                    @Override
                    public void memberRemoved( final MembershipEvent membershipEvent )
                    {
                        election.triggerElection( membershipEvent.getMembers() );
                    }

                    @Override
                    public void memberAttributeChanged( MemberAttributeEvent memberAttributeEvent )
                    {

                    }

                    @Override
                    public void init( final InitialMembershipEvent event )
                    {
                        election.triggerElection( event.getMembers() );
                    }
                } );
            }
        } );
        this.election = election;
    }

    public void forceElections()
    {
        election.triggerElection( hazelcast.members() );
    }
}
