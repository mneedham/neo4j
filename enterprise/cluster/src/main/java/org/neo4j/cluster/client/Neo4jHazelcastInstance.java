package org.neo4j.cluster.client;


import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Arrays;
import java.util.List;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class Neo4jHazelcastInstance extends LifecycleAdapter
{
    private ClusterClient.Configuration config;
    private HazelcastInstance hazelcastInstance;


    public Neo4jHazelcastInstance( ClusterClient.Configuration config )
    {
        this.config = config;
    }

    @Override
    public void start() throws Throwable
    {
        hazelcastInstance = createHazelcastInstance();

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


    private HazelcastInstance createHazelcastInstance()
    {
        JoinConfig joinConfig = new JoinConfig();
        joinConfig.getMulticastConfig().setEnabled( false );
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
        tcpIpConfig.setEnabled( true );


        List<HostnamePort> hostnamePorts = Arrays.asList(
                new HostnamePort( "192.168.33.1:5701" ),
                new HostnamePort( "192.168.33.1:5702" ),
                new HostnamePort( "192.168.33.1:5703" ) );
//        List<HostnamePort> hostnamePorts = config.get( ClusterSettings.initial_hosts );
        for ( HostnamePort hostnamePort : hostnamePorts )
        {
            tcpIpConfig.addMember( hostnamePort.getHost()+":" + hostnamePort.getPort() );
        }

        NetworkConfig networkConfig = new NetworkConfig();
        networkConfig.setPort( config.getAddress().getPort() + 700 );
        networkConfig.setJoin( joinConfig );
        //        networkConfig.getInterfaces().setInterfaces( Arrays.asList( "127.0.0.1" ) ).setEnabled( true );
        //        networkConfig.getInterfaces().setInterfaces( Arrays.asList( "192.168.1.12" ) ).setEnabled( true );
        //        networkConfig.getInterfaces()
        //                .setInterfaces( Arrays.asList(config.get( ClusterSettings.cluster_server ).getHost()) )
        //                .setEnabled( true );
        String instanceName = String.valueOf( config.getServerId().toIntegerIndex() );
        System.out.println( "instanceName = " + instanceName );
        com.hazelcast.config.Config c = new com.hazelcast.config.Config( instanceName );
        c.setProperty( "hazelcast.initial.min.cluster.size", "2" );
        c.setNetworkConfig( networkConfig );

        System.out.println( "creating HC instance with " + c.getInstanceName() );

        MemberAttributeConfig memberAttributeConfig = new MemberAttributeConfig();
        memberAttributeConfig.setIntAttribute( "server_id",  config.getServerId().toIntegerIndex());
        c.setMemberAttributeConfig( memberAttributeConfig );

        return Hazelcast.newHazelcastInstance( c );
    }

    public String getName()
    {
        return hazelcastInstance.getName();
    }

}
