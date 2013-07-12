package org.neo4j.cluster.com.message;

import org.junit.Test;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.lifecycle.LifeSupport;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;


public class NetworkInstanceIT {
    @Test
    public void canConnectThroughAllInterfacesOnPort5001IfClusterServerNotExplicitlySet() throws IOException {
        CountDownLatch latch = new CountDownLatch( 1 );
        LifeSupport life = new LifeSupport();
        NetworkInstanceSendAndReceiveTest.Server server1 = new NetworkInstanceSendAndReceiveTest.Server( latch, MapUtil.stringMap( ClusterSettings.server_id.name(), "1",
                ClusterSettings.initial_hosts.name(), "localhost:1235,localhost:5001" ) );
        life.add( server1 );
        life.start();

        List<NetworkInterface> networkInterfaces = Collections.list(NetworkInterface.getNetworkInterfaces());
        assertTrue(networkInterfaces.size() > 0);

        for (NetworkInterface networkInterface : networkInterfaces) {
            ArrayList<InetAddress> addresses = Collections.list(networkInterface.getInetAddresses());
            for (InetAddress inetAddress : addresses) {
                boolean canConnect = canConnectTo5001(inetAddress);
                assertTrue(String.format("Failed to create socket connection to *.5001 using %s [%s]", networkInterface.getDisplayName(), inetAddress), canConnect);
            }
        }
    }

    private boolean canConnectTo5001(InetAddress inetAddress) throws IOException {
        try {
            new Socket(inetAddress, 5001);
            return true;
        } catch(ConnectException ex) {
            return false;
        }
    }
}