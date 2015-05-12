package org.neo4j.coreedge;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class Cluster
{
    private File parentDir;
    private Set<CoreGraphDatabaseProxy> coreServers = new HashSet<>();
    private Set<EdgeGraphDatabaseProxy> edgeServers = new HashSet<>();
    private HazelcastClusterManagement management;

    public Cluster( File parentDir )
    {
        this.parentDir = parentDir;

        Map<String,String> params = stringMap();
        params.put( ClusterSettings.cluster_name.name(), coreClusterName() );
        Config config = new Config( params );
        this.management = new HazelcastClusterManagement( config );
    }

    public void start( int noOfCoreServers, int noOfEdgeServers )
    {
        String initialHosts = getInitialHosts( noOfCoreServers );
        GraphDatabaseFacadeFactory.Dependencies dependencies = GraphDatabaseDependencies.newDependencies();


        for ( int i = 0; i < noOfCoreServers; i++ )
        {
            int port = 5000 + i;
            Map<String,String> params = stringMap();
            params.put( ClusterSettings.cluster_server.name(), "localhost:" + port );
            params.put( "org.neo4j.server.database.mode", "CORE_EDGE" );
            params.put( ClusterSettings.server_id.name(), String.valueOf( i ) );
            params.put( ClusterSettings.initial_hosts.name(), initialHosts );
            params.put( ClusterSettings.cluster_name.name(), coreClusterName() );
            params.put( ClusterSettings.server_type.name(), "core" );

            // start up a core server

            File storeDir = new File( parentDir, "server-core-" + port );

            CoreGraphDatabaseProxy dbProxy = new CoreGraphDatabaseProxy( management, storeDir, params, dependencies );
            coreServers.add( dbProxy );
        }

        for ( int i = 0; i < noOfEdgeServers; i++ )
        {
            // start up an edge server

            Map<String,String> params = stringMap();
            params.put( "org.neo4j.server.database.mode", "CORE_EDGE" );
            params.put( ClusterSettings.server_id.name(), String.valueOf( i ) );
            params.put( ClusterSettings.initial_hosts.name(), initialHosts );
            params.put( ClusterSettings.cluster_name.name(), coreClusterName() );
            params.put( ClusterSettings.server_type.name(), "edge" );

            File storeDir = new File( parentDir, "server-edge-" + i );

            EdgeGraphDatabaseProxy dbProxy = new EdgeGraphDatabaseProxy( management, storeDir, params, dependencies );
            this.edgeServers.add( dbProxy );
        }
    }

    private String getInitialHosts( int noOfCoreServers )
    {
        StringBuilder initialHosts = new StringBuilder();
        for ( int i1 = 0; i1 < noOfCoreServers; i1++ )
        {
            int port = 5000 + i1;
            initialHosts.append( "localhost:" ).append( port );

            if ( i1 != noOfCoreServers - 1 )
            {
                initialHosts.append( "," );
            }
        }
        return initialHosts.toString();
    }

    private String coreClusterName()
    {
        return "core-neo4j";
    }

    public int getCoreServers()
    {
        return management.getNumberOfCoreServers();
    }

    public int getEdgeServers()
    {
        return management.getNumberOfEdgeServers();
    }

    public void waitAllAvailable()
    {
        for ( CoreGraphDatabaseProxy coreServer : coreServers )
        {
            coreServer.get();
        }

    }

    public void shutdown()
    {
        for ( CoreGraphDatabaseProxy coreServer : coreServers )
        {
            coreServer.get().shutdown();
        }

        for ( EdgeGraphDatabaseProxy edgeServer : edgeServers )
        {
            edgeServer.get().shutdown();
        }
    }

    private static final class CoreGraphDatabaseProxy
    {
        private final ExecutorService executor;
        private CoreGraphDatabase result;
        private Future<CoreGraphDatabase> untilThen;

        public CoreGraphDatabaseProxy( final HazelcastClusterManagement management, final File storeDir,
                final Map<String,String> params, final GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            Callable<CoreGraphDatabase> starter = new Callable<CoreGraphDatabase>()
            {
                @Override
                public CoreGraphDatabase call() throws Exception
                {
                    return new CoreGraphDatabase( management, storeDir.getAbsolutePath(), params, dependencies );
                }
            };

            executor = Executors.newFixedThreadPool( 1 );
            untilThen = executor.submit( starter );
        }

        public CoreGraphDatabase get()
        {
            if ( result == null )
            {
                try
                {
                    result = untilThen.get();
                }
                catch ( InterruptedException | ExecutionException e )
                {
                    throw new RuntimeException( e );
                }
                finally
                {
                    executor.shutdownNow();
                }
            }
            return result;
        }
    }

    private static final class EdgeGraphDatabaseProxy
    {
        private final ExecutorService executor;
        private EdgeGraphDatabase result;
        private Future<EdgeGraphDatabase> untilThen;

        public EdgeGraphDatabaseProxy( final HazelcastClusterManagement management, final File storeDir,
                final Map<String,String> params, final GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            Callable<EdgeGraphDatabase> starter = new Callable<EdgeGraphDatabase>()
            {
                @Override
                public EdgeGraphDatabase call() throws Exception
                {
                    return new EdgeGraphDatabase( management, storeDir.getAbsolutePath(), params, dependencies );
                }
            };

            executor = Executors.newFixedThreadPool( 1 );
            untilThen = executor.submit( starter );
        }

        public EdgeGraphDatabase get()
        {
            if ( result == null )
            {
                try
                {
                    result = untilThen.get();
                }
                catch ( InterruptedException | ExecutionException e )
                {
                    throw new RuntimeException( e );
                }
                finally
                {
                    executor.shutdownNow();
                }
            }
            return result;
        }
    }
}
