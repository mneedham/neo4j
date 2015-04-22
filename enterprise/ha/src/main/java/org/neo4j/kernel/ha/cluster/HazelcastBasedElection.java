package org.neo4j.kernel.ha.cluster;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.MemberImpl;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.kernel.ha.factory.Neo4jHazelcastInstance;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class HazelcastBasedElection extends LifecycleAdapter implements Election
{
    private List<HighAvailabilityMemberListener> listeners = new ArrayList<>();
    private Neo4jHazelcastInstance instance;
    private String registrationId;

    public HazelcastBasedElection( Neo4jHazelcastInstance instance )
    {
        this.instance = instance;
    }

    @Override
    public void start() throws Throwable
    {
        Cluster cluster = instance.getHazelcastInstance().getCluster();
        registrationId = cluster.addMembershipListener( new MyMembershipListener() );
    }

    @Override
    public void stop() throws Throwable
    {
        Cluster cluster = instance.getHazelcastInstance().getCluster();
        cluster.removeMembershipListener( registrationId );
    }

    @Override
    public void demote( InstanceId node )
    {

    }

    @Override
    public void performRoleElections()
    {
        List<Member> members = new ArrayList<>( instance.getHazelcastInstance().getCluster().getMembers() );
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
            MemberImpl masterMember = (MemberImpl) master;
            listener.masterIsElected(
                    new HighAvailabilityMemberChangeEvent(
                            HighAvailabilityMemberState.PENDING,
                            HighAvailabilityMemberState.TO_MASTER,
                            new InstanceId( masterMember.getId() ),
                            URI.create( String.format( "cluster://%s:%s", masterMember.getInetAddress(), 6001 ) ) ) );
        }
    }

    @Override
    public void promote( InstanceId node, String role )
    {

    }

    private class MyMembershipListener implements MembershipListener, InitialMembershipListener
    {
        @Override
        public void memberAdded( MembershipEvent membershipEvent )
        {
            System.out.println( "Member added: " + membershipEvent );
            performRoleElections();
        }

        @Override
        public void memberRemoved( MembershipEvent membershipEvent )
        {
            System.out.println( "Member removed: " + membershipEvent );
            performRoleElections();
        }

        @Override
        public void memberAttributeChanged( MemberAttributeEvent memberAttributeEvent )
        {
            System.out.println( "Member attribute changed: " + memberAttributeEvent );
            performRoleElections();
        }

        @Override
        public void init( InitialMembershipEvent event )
        {
            System.out.println( "Init: " + event );
            performRoleElections();
        }
    }
}
