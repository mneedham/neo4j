/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.scenarios;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.raft.state.DurableStateStorage;
import org.neo4j.coreedge.raft.state.DurableStateStorageImporter;
import org.neo4j.coreedge.raft.state.id_allocation.IdAllocationState;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.KernelEventHandlers;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.Matchers.greaterThan;

import static org.neo4j.coreedge.server.CoreEdgeClusterSettings.raft_advertised_address;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.kernel.impl.store.id.IdType.ARRAY_BLOCK;
import static org.neo4j.kernel.impl.store.id.IdType.LABEL_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.LABEL_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.NEOSTORE_BLOCK;
import static org.neo4j.kernel.impl.store.id.IdType.NODE;
import static org.neo4j.kernel.impl.store.id.IdType.NODE_LABELS;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY_KEY_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY_KEY_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_GROUP;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_TYPE_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_TYPE_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.SCHEMA;
import static org.neo4j.kernel.impl.store.id.IdType.STRING_BLOCK;
import static org.neo4j.test.Assert.assertEventually;

public class ImportNonCoreEdgeStoreIT
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    private Cluster cluster;

    @After
    public void shutdown()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldReplicateTransactionToCoreServers() throws Exception
    {
        // given
        File dbDir = dir.directory();
        System.out.println( "dbDir = " + dbDir );
        FileUtils.deleteRecursively( dbDir );

        File classicNeo4jStore = createClassicNeo4jStore( dbDir, 2000 );

        Collection<String> filesToExclude = filesToExclude( classicNeo4jStore );
        for ( int serverId = 0; serverId < 3; serverId++ )
        {
            copyStore( classicNeo4jStore, dbDir, serverId, filesToExclude );
            addIdAllocationState(dbDir, serverId);
        }

        cluster = Cluster.start( dbDir, 3, 0 );


        // when
        GraphDatabaseService coreDB = cluster.findLeader( 5000 );

        try ( Transaction tx = coreDB.beginTx() )
        {
            Node node = coreDB.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        }

        // then
        for ( final CoreGraphDatabase db : cluster.coreServers() )
        {
            try ( Transaction tx = db.beginTx() )
            {
                ThrowingSupplier<Long, Exception> nodeCount = () -> count( db.getAllNodes() );

                Config config = db.getDependencyResolver().resolveDependency( Config.class );

                assertEventually( "node to appear on core server " + config.get( raft_advertised_address ), nodeCount,
                        greaterThan( 0L ), 15, SECONDS );

                Assert.assertEquals( 2001, IteratorUtil.count( GlobalGraphOperations.at( db ).getAllNodes() ) );

                tx.success();
            }
        }
    }

    private void addIdAllocationState( File base, int serverId ) throws IOException
    {
        File coreDir = new File( base, "server-core-" + serverId );
        File clusterStateDirectory = new File( coreDir, "cluster-state" );

        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        DefaultIdGeneratorFactory factory = new DefaultIdGeneratorFactory( fileSystem );

        int[] lastIdRangeLengthForMe = new int[]{
                NODE.getGrabSize(),
                RELATIONSHIP.getGrabSize(),
                PROPERTY.getGrabSize(),
                STRING_BLOCK.getGrabSize(),
                ARRAY_BLOCK.getGrabSize(),
                PROPERTY_KEY_TOKEN.getGrabSize(),
                PROPERTY_KEY_TOKEN_NAME.getGrabSize(),
                RELATIONSHIP_TYPE_TOKEN.getGrabSize(),
                RELATIONSHIP_TYPE_TOKEN_NAME.getGrabSize(),
                LABEL_TOKEN.getGrabSize(),
                LABEL_TOKEN_NAME.getGrabSize(),
                NEOSTORE_BLOCK.getGrabSize(),
                SCHEMA.getGrabSize(),
                NODE_LABELS.getGrabSize(),
                RELATIONSHIP_GROUP.getGrabSize()};

        long[] highIds = new long[]{
                getHighId( coreDir, factory, NODE, StoreFactory.NODE_STORE_NAME ),
                getHighId( coreDir, factory, RELATIONSHIP, StoreFactory.RELATIONSHIP_STORE_NAME ),
                getHighId( coreDir, factory, PROPERTY, StoreFactory.PROPERTY_STORE_NAME ),
                getHighId( coreDir, factory, STRING_BLOCK, StoreFactory.PROPERTY_STRINGS_STORE_NAME ),
                getHighId( coreDir, factory, ARRAY_BLOCK, StoreFactory.PROPERTY_ARRAYS_STORE_NAME ),
                getHighId( coreDir, factory, PROPERTY_KEY_TOKEN, StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME ),
                getHighId( coreDir, factory, PROPERTY_KEY_TOKEN_NAME, StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME ),
                getHighId( coreDir, factory, RELATIONSHIP_TYPE_TOKEN, StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME ),
                getHighId( coreDir, factory, RELATIONSHIP_TYPE_TOKEN_NAME, StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME ),
                getHighId( coreDir, factory, LABEL_TOKEN, StoreFactory.LABEL_TOKEN_STORE_NAME ),
                getHighId( coreDir, factory, LABEL_TOKEN_NAME, StoreFactory.LABEL_TOKEN_NAMES_STORE_NAME ),
//                getHighId( coreDir, factory, NEOSTORE_BLOCK, MetaDataStore.DEFAULT_NAME ),
                -1,
                getHighId( coreDir, factory, SCHEMA, StoreFactory.SCHEMA_STORE_NAME ),
                getHighId( coreDir, factory, NODE_LABELS, StoreFactory.NODE_LABELS_STORE_NAME ),
                getHighId( coreDir, factory, RELATIONSHIP_GROUP, StoreFactory.RELATIONSHIP_GROUP_STORE_NAME )};

        IdAllocationState state = new IdAllocationState( highIds, highIds, lastIdRangeLengthForMe, -1 );

        DurableStateStorageImporter<IdAllocationState> storage = new DurableStateStorageImporter<>(
                fileSystem, new File( clusterStateDirectory, "id-allocation-state" ), "id-allocation",
                new IdAllocationState.Marshal(),
                1000, () -> new DatabaseHealth(
                new DatabasePanicEventGenerator( new KernelEventHandlers( NullLog.getInstance() ) ),
                NullLog.getInstance() ), NullLogProvider.getInstance() );

        storage.persist( state );
    }

    private long getHighId( File coreDir, DefaultIdGeneratorFactory factory, IdType idType, String store )
    {
        return factory.open( new File( coreDir, idFile( store ) ), idType.getGrabSize(), idType, -1 ).getHighId();
    }

    private String idFile( String store )
    {
        return MetaDataStore.DEFAULT_NAME + store + ".id";
    }

    private Collection<String> filesToExclude( File classicNeo4jStore )
    {
        Collection<String> filesToExclude = allTheTransactionLogsIn( classicNeo4jStore );
        Collections.addAll( filesToExclude, "neostore", "neostore.id" );
        return filesToExclude;
    }

    private Set<String> allTheTransactionLogsIn( File dbDir )
    {
        Set<String> results = new HashSet<>();
        for ( String s : dbDir.list((dir, file) -> file.startsWith( "neostore.transaction.db" )) )
        {
            if ( s.startsWith( "neostore.transaction.db" ) )
            {
                results.add( s );
            }
        }
        return results;
    }

    private File copyStore( File store, File base, int serverId, Collection<String> excludedFiles )
            throws IOException
    {
        File coreDir = new File( base, "server-core-" + serverId );

        FileUtils.copyRecursively( store, coreDir, pathname -> !excludedFiles.contains( pathname.getName() ) );

        return coreDir;
    }


    private File createClassicNeo4jStore( File base, int nodesToCreate )
    {
        File existingDbDir = new File( base, "existing" );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( existingDbDir );

        for ( int i = 0; i < (nodesToCreate / 2); i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node1 = db.createNode( Label.label( "Label-" + i ) );
                Node node2 = db.createNode( Label.label( "Label-" + i ) );
                node1.createRelationshipTo( node2, RelationshipType.withName( "REL-" + i ) );
                tx.success();
            }
        }

        db.shutdown();

        return existingDbDir;
    }
}
