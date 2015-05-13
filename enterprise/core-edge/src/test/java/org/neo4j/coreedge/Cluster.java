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

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class Cluster
{
    private static final String CLUSTER_NAME = "core-neo4j";

    private final File parentDir;
    private Set<CoreGraphDatabase> coreServers = new HashSet<>();
    private Set<EdgeGraphDatabase> edgeServers = new HashSet<>();
    private HazelcastClusterManagement management;

    public static Cluster start( File parentDir, int noOfCoreServers, int noOfEdgeServers )
            throws ExecutionException, InterruptedException
    {
        return new Cluster( parentDir, noOfCoreServers, noOfEdgeServers );
    }

    Cluster( File parentDir, int noOfCoreServers, int noOfEdgeServers )
            throws ExecutionException, InterruptedException
    {
        this.parentDir = parentDir;
        ExecutorService executor = Executors.newCachedThreadPool();
        try
        {
            startServers( executor, noOfCoreServers, noOfEdgeServers );
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    private void startServers(
            ExecutorService executor,
            int noOfCoreServers, int noOfEdgeServers ) throws ExecutionException, InterruptedException
    {
        String[] initialHosts = buildInitialHosts( noOfCoreServers );
        this.management = new HazelcastClusterManagement( CLUSTER_NAME, initialHosts );

        final GraphDatabaseFacadeFactory.Dependencies dependencies = GraphDatabaseDependencies.newDependencies();

        String initialHostsConfig = ArrayUtil.join( initialHosts, "," );

        List<Callable<CoreGraphDatabase>> coreServerSuppliers = new ArrayList<>();
        for ( int i = 0; i < noOfCoreServers; i++ )
        {
            // start up a core server
            int port = 5000 + i;
            final Map<String,String> params = serverParams( "CORE", i, initialHostsConfig );
            params.put( ClusterSettings.cluster_server.name(), "localhost:" + port );

            final File storeDir = new File( parentDir, "server-core-" + port );

            coreServerSuppliers.add( new Callable<CoreGraphDatabase>()
            {
                @Override
                public CoreGraphDatabase call()
                {
                    return new CoreGraphDatabase( management, storeDir, params, dependencies );
                }
            } );
        }

        List<Future<CoreGraphDatabase>> coreServerFutures = executor.invokeAll( coreServerSuppliers );
        for ( Future<CoreGraphDatabase> coreServer : coreServerFutures )
        {
            this.coreServers.add( coreServer.get() );
        }

        List<Callable<EdgeGraphDatabase>> edgeServerSuppliers = new ArrayList<>();
        for ( int i = 0; i < noOfEdgeServers; i++ )
        {
            // start up an edge server
            final Map<String,String> params = serverParams( "EDGE", i, initialHostsConfig );

            final File storeDir = new File( parentDir, "server-edge-" + i );

            edgeServerSuppliers.add( new Callable<EdgeGraphDatabase>()
            {
                @Override
                public EdgeGraphDatabase call()
                {
                    return new EdgeGraphDatabase( management, storeDir, params, dependencies );
                }
            } );
        }

        List<Future<EdgeGraphDatabase>> edgeServerFutures = executor.invokeAll( edgeServerSuppliers );

        for ( Future<EdgeGraphDatabase> edgeServer : edgeServerFutures )
        {
            this.edgeServers.add( edgeServer.get() );
        }
    }

    public int getCoreServers()
    {
        return management.getNumberOfCoreServers();
    }

    public int getEdgeServers()
    {
        return management.getNumberOfEdgeServers();
    }

    public void shutdown()
    {
        for ( CoreGraphDatabase coreServer : coreServers )
        {
            coreServer.shutdown();
        }

        for ( EdgeGraphDatabase edgeServer : edgeServers )
        {
            edgeServer.shutdown();
        }
    }

    public void removeCoreServer()
    {
        CoreGraphDatabase aCoreServer = findExistingCoreServer();
        aCoreServer.shutdown();
        coreServers.remove( aCoreServer );
    }

    public void removeEdgeServer()
    {
        EdgeGraphDatabase anEdgeServer = findExistingEdgeServer();
        anEdgeServer.shutdown();
        edgeServers.remove( anEdgeServer );
    }

    public void addCoreServer( int serverId )
    {
        Config config = findExistingCoreServer().getDependencyResolver().resolveDependency( Config.class );
        HostnamePort hostnamePort = config.get( ClusterSettings.cluster_server );
        String initialHostsConfig = hostnamePort.getHost() + ":" + hostnamePort.getPort();

        // start up a core server
        int port = 5000 + serverId;
        final Map<String,String> params = serverParams( "CORE", serverId, initialHostsConfig );
        params.put( ClusterSettings.cluster_server.name(), "localhost:" + port );

        final File storeDir = new File( parentDir, "server-core-" + port );

        CoreGraphDatabase coreGraphDatabase =
                new CoreGraphDatabase( management, storeDir, params, GraphDatabaseDependencies.newDependencies() );
        coreServers.add(coreGraphDatabase);
    }

    private CoreGraphDatabase findExistingCoreServer()
    {
        return coreServers.iterator().next();
    }

    private EdgeGraphDatabase findExistingEdgeServer()
    {
        return edgeServers.iterator().next();
    }

    private static Map<String,String> serverParams( String serverType, int serverId, String initialHosts )
    {
        Map<String,String> params = stringMap();
        params.put( "org.neo4j.server.database.mode", "CORE_EDGE" );
        params.put( ClusterSettings.cluster_name.name(), CLUSTER_NAME );
        params.put( ClusterSettings.server_type.name(), serverType );
        params.put( ClusterSettings.server_id.name(), String.valueOf( serverId ) );
        params.put( ClusterSettings.initial_hosts.name(), initialHosts );
        return params;
    }

    private static String[] buildInitialHosts( int noOfCoreServers )
    {
        String[] initialHosts = new String[noOfCoreServers];
        for ( int i = 0; i < noOfCoreServers; i++ )
        {
            int port = 5000 + i;
            initialHosts[i] = "localhost:" + port;
        }
        return initialHosts;
    }
}
