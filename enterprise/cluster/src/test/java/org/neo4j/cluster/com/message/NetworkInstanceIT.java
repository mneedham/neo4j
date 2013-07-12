/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
        Server server1 = new Server( latch, MapUtil.stringMap( ClusterSettings.server_id.name(), "1",
                ClusterSettings.initial_hosts.name(), "localhost:1235,localhost:5001" ) );
        life.add( server1 );
        life.start();

        List<NetworkInterface> networkInterfaces = Collections.list(NetworkInterface.getNetworkInterfaces());
        assertTrue(networkInterfaces.size() > 0);

        for (NetworkInterface networkInterface : networkInterfaces) {
            ArrayList<InetAddress> addresses = Collections.list(networkInterface.getInetAddresses());
            for (InetAddress inetAddress : addresses) {
                System.out.println("inetAddress = " + inetAddress);
                boolean canConnect = canConnectTo5001(inetAddress);
                assertTrue(String.format("Failed to create socket connection to *.5001 using %s [%s]", networkInterface.getDisplayName(), inetAddress), canConnect);
            }
        }
    }

    private boolean canConnectTo5001(InetAddress inetAddress) throws IOException {
        Socket socket = null;
        try {
            socket = new Socket(inetAddress, 5001);
            return true;
        } catch(ConnectException ex) {
            return false;
        } finally {
            if(socket != null) {
                socket.close();
            }
        }
    }
}