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
    public void canConnectThroughAllLoopbackInterfacesOnPort5001IfClusterServerNotExplicitlySet() throws IOException {
        CountDownLatch latch = new CountDownLatch( 1 );
        LifeSupport life = new LifeSupport();
        NetworkInstanceSendAndReceiveTest.Server server1 = new NetworkInstanceSendAndReceiveTest.Server( latch, MapUtil.stringMap(ClusterSettings.server_id.name(), "1",
                ClusterSettings.initial_hosts.name(), "localhost:1235,localhost:5001") );
        life.add( server1 );
        life.start();

        NetworkInterface localLoopback = getLocalLoopbackInterface(Collections.list(NetworkInterface.getNetworkInterfaces()));
        ArrayList<InetAddress> localLoopBackAddresses = Collections.list(localLoopback.getInetAddresses());
        assertTrue(localLoopBackAddresses.size() > 0);
        for (InetAddress inetAddress : localLoopBackAddresses) {
            boolean canConnect = canConnectTo5001(inetAddress);
            assertTrue(String.format("Failed to create socket connection to *.5001 using %s [%s]", localLoopback.getDisplayName(), inetAddress), canConnect);
        }
    }

    @Test
    public void canConnectThroughAtLeastOneExternalInterfaceOnPort5001IfClusterServerNotExplicitlySet() throws IOException {
        CountDownLatch latch = new CountDownLatch( 1 );
        LifeSupport life = new LifeSupport();
        NetworkInstanceSendAndReceiveTest.Server server1 = new NetworkInstanceSendAndReceiveTest.Server( latch, MapUtil.stringMap( ClusterSettings.server_id.name(), "1",
                ClusterSettings.initial_hosts.name(), "localhost:1235,localhost:5001" ) );
        life.add( server1 );
        life.start();

        List<NetworkInterface> nonLoopbackInterfaces = getNonLoopbackInterfaces(Collections.list(NetworkInterface.getNetworkInterfaces()));
        assertTrue(nonLoopbackInterfaces.size() > 0);

        boolean canConnectThroughAtLeastOne = false;
        for (NetworkInterface networkInterface : nonLoopbackInterfaces) {
            ArrayList<InetAddress> nonLoopBackAddresses = Collections.list(networkInterface.getInetAddresses());
            for (InetAddress inetAddress : nonLoopBackAddresses) {
                canConnectThroughAtLeastOne |= canConnectTo5001(inetAddress);
            }
        }
        assertTrue(canConnectThroughAtLeastOne);
    }

    private List<NetworkInterface> getNonLoopbackInterfaces(ArrayList<NetworkInterface> networkInterfaces) throws SocketException {
        List<NetworkInterface> nonLoopback = new ArrayList<NetworkInterface>();
        for (NetworkInterface networkInterface : networkInterfaces) {
            if(!networkInterface.isLoopback()) {
                nonLoopback.add(networkInterface);
            }
        }
        return nonLoopback;
    }

    private boolean canConnectTo5001(InetAddress inetAddress) throws IOException {
        try {
            new Socket(inetAddress, 5001);
            return true;
        } catch(ConnectException ex) {
            return false;
        }
    }

    private NetworkInterface getLocalLoopbackInterface(ArrayList<NetworkInterface> networkInterfaces) throws SocketException {
        for (NetworkInterface networkInterface : networkInterfaces) {
            if(networkInterface.isLoopback()) {
                return networkInterface;
            }
        }
        throw new RuntimeException("No loopback interface");
    }


}
