package org.neo4j.coreedge;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

import java.util.List;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class HazelcastClientLifeCycle extends LifecycleAdapter
{
    private Config config;
    private HazelcastInstance hazelcastInstance;

    public HazelcastClientLifeCycle( Config config )
    {
        this.config = config;
    }

    @Override
    public void start() throws Throwable
    {
        List<HostnamePort> hostnamePorts = config.get( ClusterSettings.initial_hosts );

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName( config.get( ClusterSettings.cluster_name ) );

        HostnamePort hostnamePort = hostnamePorts.get( 0 );
        clientConfig.getNetworkConfig().addAddress( hostnamePort.getHost() + ":" + hostnamePort.getPort() );

        hazelcastInstance = HazelcastClient.newHazelcastClient( clientConfig );
        hazelcastInstance
                .getMap( HazelcastClusterManagement.EDGE_SERVERS )
                .put( config.get( ClusterSettings.server_id ), 1 );
    }

    @Override
    public void stop() throws Throwable
    {
        try
        {
            hazelcastInstance
                    .getMap( HazelcastClusterManagement.EDGE_SERVERS )
                    .remove( config.get( ClusterSettings.server_id ) );
            hazelcastInstance.shutdown();
        }
        catch ( RuntimeException ignored )
        {
            // this can happen if the edge server is trying to shutdown but
            // the core is gone
        }
    }
}
