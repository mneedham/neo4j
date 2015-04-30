package org.neo4j.kernel.ha.factory;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;

import org.neo4j.cluster.client.HazelcastLifecycle;
import org.neo4j.ha.MyElection;

public class ElectionInstigator
{
    private final HazelcastLifecycle hazelcast;
    private final MyElection election;

    public ElectionInstigator( HazelcastLifecycle hazelcast, MyElection election )
    {
        this.hazelcast = hazelcast;
        this.hazelcast.addStartupListener( new HazelcastLifecycle.StartupListener()
        {
            @Override
            public void hazelcastStarted( HazelcastInstance hazelcastInstance )
            {
                hazelcastInstance.getCluster().addMembershipListener( new InitialMembershipListener()
                {
                    private final MyElection myElection = ElectionInstigator.this.election;

                    @Override
                    public void memberAdded( final MembershipEvent membershipEvent )
                    {
                        myElection.triggerElection( membershipEvent.getMembers() );
                    }

                    @Override
                    public void memberRemoved( final MembershipEvent membershipEvent )
                    {
                        myElection.triggerElection( membershipEvent.getMembers() );
                    }

                    @Override
                    public void memberAttributeChanged( MemberAttributeEvent memberAttributeEvent )
                    {

                    }

                    @Override
                    public void init( final InitialMembershipEvent event )
                    {
                        myElection.triggerElection( event.getMembers() );
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
