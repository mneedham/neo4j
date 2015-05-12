package org.neo4j.coreedge;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.List;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;

public class HazelcastClusterManagement implements ClusterManagement
{

    private final Config config;

    public HazelcastClusterManagement( Config config )
    {

        this.config = config;
    }

    @Override
    public int getNumberOfCoreServers()
    {
        int noCoreServers = 0;
        for ( HazelcastInstance instance : Hazelcast.getAllHazelcastInstances() )
        {
            if ( instance.getConfig().getGroupConfig().getName().equals( config.get( ClusterSettings.cluster_name ) ) )
            {
                noCoreServers++;
            }

        }
        return noCoreServers;
    }

    @Override
    public int getNumberOfEdgeServers()
    {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName( config.get( ClusterSettings.cluster_name ) );

        List<HostnamePort> hostnamePorts = config.get( ClusterSettings.initial_hosts );

        for ( HostnamePort hostnamePort : hostnamePorts )
        {
            clientConfig.getNetworkConfig().addAddress( hostnamePort.getHost() + ":" + hostnamePort.getPort() );
        }

        HazelcastInstance client = HazelcastClient.newHazelcastClient( clientConfig );

        return client.getMap( "edge-servers" ).size();
    }
}
