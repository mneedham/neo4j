package org.neo4j.cluster.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipListener;

import org.neo4j.cluster.InstanceId;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class HazelcastLifecycle extends LifecycleAdapter
{
    private ClusterClient.Configuration config;
    private HazelcastInstance hazelcastInstance;

    private List<StartupListener> startupListeners = new ArrayList<>();

    public HazelcastLifecycle( ClusterClient.Configuration config )
    {
        this.config = config;
    }

    @Override
    public void start() throws Throwable
    {
        hazelcastInstance = createHazelcastInstance();

        for ( StartupListener startupListener : startupListeners )
        {
            startupListener.hazelcastStarted( hazelcastInstance );
        }
    }

    @Override
    public void stop() throws Throwable
    {
        hazelcastInstance.shutdown();
    }

    private HazelcastInstance createHazelcastInstance()
    {
        JoinConfig joinConfig = new JoinConfig();
        joinConfig.getMulticastConfig().setEnabled( false );
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
        tcpIpConfig.setEnabled( true );


        for ( HostnamePort hostnamePort : config.getInitialHosts() )
        {
            tcpIpConfig.addMember( hostnamePort.getHost() + ":" + (hostnamePort.getPort() + 700) );
        }

        NetworkConfig networkConfig = new NetworkConfig();
        networkConfig.setPort( config.getAddress().getPort() + 700 );
        networkConfig.setJoin( joinConfig );
        String instanceName = String.valueOf( config.getServerId().toIntegerIndex() );
        System.out.println( "instanceName = " + instanceName );
        com.hazelcast.config.Config c = new com.hazelcast.config.Config( instanceName );
        c.setProperty( "hazelcast.initial.min.cluster.size", "2" );
        c.setNetworkConfig( networkConfig );

        System.out.println( "creating HC instance with " + c.getInstanceName() );

        MemberAttributeConfig memberAttributeConfig = new MemberAttributeConfig();
        memberAttributeConfig.setIntAttribute( "server_id", config.getServerId().toIntegerIndex() );
        c.setMemberAttributeConfig( memberAttributeConfig );

        return Hazelcast.newHazelcastInstance( c );
    }

    public String addMembershipListener( MembershipListener membershipListener )
    {
        return hazelcastInstance.getCluster().addMembershipListener( membershipListener );
    }

    public interface StartupListener
    {
        void hazelcastStarted( HazelcastInstance hazelcastInstance );
    }

    public void addStartupListener( StartupListener startupListener )
    {
        startupListeners.add( startupListener );
    }

    public Set<Member> members()
    {
        return hazelcastInstance.getCluster().getMembers();
    }

    public InstanceId myId()
    {
        return new InstanceId( hazelcastInstance.getCluster().getLocalMember().getIntAttribute( "server_id" ) );
    }
}
