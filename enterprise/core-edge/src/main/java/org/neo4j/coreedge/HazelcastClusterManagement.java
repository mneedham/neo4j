/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.List;

public class HazelcastClusterManagement implements ClusterManagement
{
    public static final String EDGE_SERVERS = "edge-servers";
    private final String clusterName;
    private final String[] knownAddresses;

    public HazelcastClusterManagement( String clusterName, String[] knownAddresses )
    {
        this.clusterName = clusterName;
        this.knownAddresses = knownAddresses;
    }

    @Override
    public int getNumberOfCoreServers()
    {
        int noCoreServers = 0;
        for ( HazelcastInstance instance : Hazelcast.getAllHazelcastInstances() )
        {
            if ( instance.getConfig().getGroupConfig().getName().equals( clusterName ) )
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
        clientConfig.getGroupConfig().setName( clusterName );

        for ( String hostnamePort : knownAddresses )
        {
            clientConfig.getNetworkConfig().addAddress( hostnamePort );
        }

        HazelcastInstance client = HazelcastClient.newHazelcastClient( clientConfig );

        return client.getMap( EDGE_SERVERS ).size();
    }
}
