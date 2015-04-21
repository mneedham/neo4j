package org.neo4j.kernel.ha.factory;

import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.HazelcastBasedElection;
import org.neo4j.kernel.ha.cluster.HighAvailability;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class Neo4jHazelcastInstance extends LifecycleAdapter implements HighAvailability
{
    private Config config;
    private HazelcastInstance hazelcastInstance;
    private HazelcastBasedElection election;
    private List<HighAvailabilityMemberListener> listeners = new ArrayList<>();

    public Neo4jHazelcastInstance( Config config )
    {
        this.config = config;
    }

    @Override
    public void start() throws Throwable
    {
        hazelcastInstance = createHazelcastInstance();
        election = new HazelcastBasedElection( hazelcastInstance, listeners );
    }

    @Override
    public void stop() throws Throwable
    {
        hazelcastInstance.shutdown();
    }

    public HazelcastInstance getHazelcastInstance()
    {
        return hazelcastInstance;
    }

    public HazelcastBasedElection election() {
        return election;
    }

    private HazelcastInstance createHazelcastInstance(  )
    {
        JoinConfig joinConfig = new JoinConfig();
        joinConfig.getMulticastConfig().setEnabled( false );
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
        tcpIpConfig.setEnabled( true );

        List<HostnamePort> hostnamePorts = config.get( ClusterSettings.initial_hosts );
        for ( HostnamePort hostnamePort : hostnamePorts )
        {
            tcpIpConfig.addMember( hostnamePort.getHost() + ":" + hostnamePort.getPort() );
        }

        NetworkConfig networkConfig = new NetworkConfig();
        networkConfig.setPort( config.get( ClusterSettings.cluster_server ).getPort() );
        networkConfig.setJoin( joinConfig );
//        networkConfig.getInterfaces().setInterfaces( Arrays.asList( "127.0.0.1" ) ).setEnabled( true );
//        networkConfig.getInterfaces().setInterfaces( Arrays.asList( "192.168.1.12" ) ).setEnabled( true );
//        networkConfig.getInterfaces()
//                .setInterfaces( Arrays.asList(config.get( ClusterSettings.cluster_server ).getHost()) )
//                .setEnabled( true );

        String instanceName = String.valueOf( config.get( ClusterSettings.server_id ).toIntegerIndex() );
        System.out.println( "instanceName = " + instanceName );
        com.hazelcast.config.Config c = new com.hazelcast.config.Config( instanceName );
        c.setProperty( "hazelcast.initial.min.cluster.size", "2" );
        c.setNetworkConfig( networkConfig );

        System.out.println("creating HC instance with " + c.getInstanceName());
        return Hazelcast.newHazelcastInstance( c );
    }

    public String getName()
    {
        return hazelcastInstance.getName();
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

}
