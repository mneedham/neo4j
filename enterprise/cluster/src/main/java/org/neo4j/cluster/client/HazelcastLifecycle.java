package org.neo4j.cluster.client;

import java.util.ArrayList;
import java.util.Arrays;
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

//    private List<HighAvailabilityMemberChangeEvent> roleListeners = new ArrayList<>();
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
//        bindRoleMap();
//        bindAvailabilityMap();
    }

//    private void bindAvailabilityMap()
//    {
//        IMap<Integer, ClusterMemberAvailabilityState> map = hazelcastInstance.getMap( MAP_AVAILABILITY );
//        map.addEntryListener( new EntryListener<Integer, ClusterMemberAvailabilityState>()
//        {
//            @Override public void entryAdded( EntryEvent<Integer, ClusterMemberAvailabilityState> event )
//            {
//
//            }
//
//            @Override public void entryRemoved( EntryEvent<Integer, ClusterMemberAvailabilityState> event )
//            {
//
//            }
//
//            @Override
//            public void entryUpdated( EntryEvent<Integer, ClusterMemberAvailabilityState> event )
//            {
//                if ( event.getKey() == myId().toIntegerIndex() )
//                {
//                    notifyAvailability( event.getValue() );
//                }
//            }
//
//            @Override public void entryEvicted( EntryEvent<Integer, ClusterMemberAvailabilityState> event )
//            {
//
//            }
//
//            @Override public void mapEvicted( MapEvent event )
//            {
//
//            }
//
//            @Override public void mapCleared( MapEvent event )
//            {
//
//            }
//        }, true );
//        ClusterMemberAvailabilityState availabilityState = map.get( myId().toIntegerIndex() );
//        notifyAvailability( availabilityState );
//    }
//
//
//
//    private void notifyAvailability( ClusterMemberAvailabilityState availabilityState )
//    {
//        for ( StartupListener availabilityListener : startupListeners )
//        {
//            if ( availabilityState.isAvailable() )
//            {
//                availabilityListener.memberIsAvailable(
//                        availabilityState.getRole(), availabilityState.getInstanceId(),
//                        availabilityState.getAtUri(), availabilityState.getStoreId() );
//            }
//            else
//            {
//                availabilityListener.memberIsUnavailable(
//                        availabilityState.getRole(), availabilityState.getInstanceId() );
//
//            }
//        }
//    }

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


        List<HostnamePort> hostnamePorts = Arrays.asList(
                new HostnamePort( "192.168.1.23:5701" ),
                new HostnamePort( "192.168.1.23:5702" ),
                new HostnamePort( "192.168.1.23:5703" ) );
//        List<HostnamePort> hostnamePorts = config.get( ClusterSettings.initial_hosts );
        for ( HostnamePort hostnamePort : hostnamePorts )
        {
            tcpIpConfig.addMember( hostnamePort.getHost() + ":" + hostnamePort.getPort() );
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
