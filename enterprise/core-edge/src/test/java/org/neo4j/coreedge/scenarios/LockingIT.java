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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cluster.InstanceId;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckTool;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import static junit.framework.TestCase.fail;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;

import static org.neo4j.cluster.ClusterSettings.server_id;
import static org.neo4j.coreedge.server.CoreEdgeClusterSettings.raft_advertised_address;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.IteratorUtil.asList;
import static org.neo4j.test.Assert.assertEventually;

public class LockingIT
{
    public static final int LABELS = 10;
    private static final int RELATIONSHIPS = 1;
    public static final int NODES = 10;

    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );


    @Test
    public void shouldCluster() throws Exception
    {
        // given
        File dbPath = new File("/tmp/" + System.currentTimeMillis());
        System.out.println( "dbPath = " + dbPath );

        Cluster cluster = Cluster.start( dbPath, 3, 0 );

        GraphDatabaseService db = cluster.findLeader( 10_000 );
        final Random random = new Random();
        AtomicBoolean running = new AtomicBoolean( true );

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < NODES; i++ )
            {
                db.createNode();
            }

            tx.success();
        }

        Thread labelsThread = new Thread( () -> {
            while ( running.get() )
            {
                for ( int j = 0; j < LABELS; j++ )
                {
                    addLabels( db, j );
                }

                for ( int j = 0; j < LABELS; j++ )
                {
                    final int tmpJ = j;
                    try ( Transaction tx = db.beginTx() )
                    {
                        for ( Node node : db.getAllNodes() )
                        {
                            node.removeLabel( label( tmpJ ) );

                            doSlowWork( node );

                        }
                        tx.success();
                    }
                    catch ( DeadlockDetectedException ex )
                    {
                        System.out.println( "deadlock in labels" );
                        // ignore for now
                    }
                }
            }
        }, "LabelThread" );

        Thread relThread1 = new Thread( () -> {
            while ( running.get() )
            {
                createRelationships( db, random );
            }
        }, "RelThread1" );

        Thread relThread2 = new Thread( () -> {
            while ( running.get() )
            {
                createRelationships( db, random );
            }
        }, "RelThread2" );

        Thread relThread3 = new Thread( () -> {
            while ( running.get() )
            {
                createRelationships( db, random );
            }
        }, "RelThread3" );

        labelsThread.start();
        relThread1.start();
        relThread2.start();
        relThread3.start();

        Thread.sleep( 60_000 );
        running.set( false );

        labelsThread.join();
        relThread1.join();
        relThread2.join();
        relThread3.join();

        cluster.shutdown();


//        ConsistencyCheckService.Result result = new ConsistencyCheckService()
//                .runFullConsistencyCheck( dbPath, new Config(),
//                        ProgressMonitorFactory.textual( System.out ), NullLogProvider.getInstance(), true );
//
//        System.out.println( "result = " + result );
    }


    @Test
    public void should() throws Exception
    {
        // given
        File dbPath = new File( "/tmp/locking" );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( dbPath );
        final Random random = new Random();
        AtomicBoolean running = new AtomicBoolean( true );

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < NODES; i++ )
            {
                db.createNode();
            }

            tx.success();
        }

        Thread labelsThread = new Thread( () -> {
            while ( running.get() )
            {
                for ( int j = 0; j < LABELS; j++ )
                {
                    addLabels( db, j );
                }

                for ( int j = 0; j < LABELS; j++ )
                {
                    final int tmpJ = j;
                    try ( Transaction tx = db.beginTx() )
                    {
                        for ( Node node : db.getAllNodes() )
                        {
                            node.removeLabel( label( tmpJ ) );

                            doSlowWork( node );

                        }
                        tx.success();
                    }
                    catch ( DeadlockDetectedException ex )
                    {
                        System.out.println( "deadlock in labels" );
                        // ignore for now
                    }
                }
            }
        }, "LabelThread" );

        Thread relThread1 = new Thread( () -> {
            while ( running.get() )
            {
                createRelationships( db, random );
            }
        }, "RelThread1" );

        Thread relThread2 = new Thread( () -> {
            while ( running.get() )
            {
                createRelationships( db, random );
            }
        }, "RelThread2" );

        Thread relThread3 = new Thread( () -> {
            while ( running.get() )
            {
                createRelationships( db, random );
            }
        }, "RelThread3" );

        labelsThread.start();
        relThread1.start();
        relThread2.start();
        relThread3.start();

        Thread.sleep( 90_000 );
        running.set( false );

        labelsThread.join();
        relThread1.join();
        relThread2.join();
        relThread3.join();

        db.shutdown();


        ConsistencyCheckService.Result result = new ConsistencyCheckService()
                .runFullConsistencyCheck( dbPath, new Config(),
                        ProgressMonitorFactory.textual( System.out ), NullLogProvider.getInstance(), true );

        System.out.println( "result = " + result );
    }

    private void doSlowWork( Node node )
    {
        long count = 0;
        for ( long i = 0; i < 10_000_000L; i++ )
        {
            count += i;
        }

        node.setProperty( "count", count );
        node.removeProperty( "count" );
    }

    private void addLabels( GraphDatabaseService db, int j )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : db.getAllNodes() )
            {
                node.addLabel( label( j ) );
            }
            tx.success();
        }
        catch ( DeadlockDetectedException ex )
        {
            System.out.println( "deadlock in labels" );
            // ignore for now
        }
    }

    private void createRelationships( GraphDatabaseService db, Random random )
    {
        for ( int i = 0; i < RELATIONSHIPS; i++ )
        {
            final int tmpI = i;

            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.getNodeById( random.nextInt( NODES ) );
                Node otherNode = db.getNodeById( random.nextInt( NODES ) );
                node.createRelationshipTo( otherNode, RelationshipType.withName( "rel-" + tmpI ) );

                doSlowWork( node );

                tx.success();
            }
            catch ( DeadlockDetectedException ex )
            {
                System.out.println( "deadlock in rels" );
                // ignore for now
            }
        }

//        try ( Transaction tx = db.beginTx() )
//        {
//            for ( Relationship relationship : db.getAllRelationships() )
//            {
//                relationship.delete();
//            }
//            tx.success();
//        }
//        catch ( DeadlockDetectedException ex )
//        {
//            System.out.println( "deadlock in rels" );
//            // ignore for now
//        }
    }

    private Label label( int j )
    {
        return Label.label( "label-" + j );
    }
}
