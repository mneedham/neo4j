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
package org.neo4j.causalclustering.stresstests;

import java.io.File;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import static org.neo4j.consistency.ConsistencyCheckTool.runConsistencyCheckTool;

class StartStopLoad extends RepeatUntilOnSelectedMemberCallable
{
    StartStopLoad( BooleanSupplier keepGoing, Runnable onFailure, Cluster cluster, int numberOfCores,
            int numberOfEdges )
    {
        super( keepGoing, onFailure, cluster, numberOfCores, numberOfEdges );
    }

    @Override
    protected void doWorkOnMember( boolean isCore, int id )
    {
        ClusterMember member = isCore ? cluster.getCoreMemberById( id ) : cluster.getReadReplicaById( id );
        String storeDir = member.database().getStoreDir();
        member.shutdown();
        assertStoreConsistent( storeDir );
        LockSupport.parkNanos( 5_000_000_000L );
        member.start();
        LockSupport.parkNanos( 5_000_000_000L );
    }

    private void assertStoreConsistent( String storeDir )
    {
        try
        {
            GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( new File( storeDir ) );
            db.shutdown();

            ConsistencyCheckService.Result result = runConsistencyCheckTool( new String[]{storeDir} );
            if ( !result.isSuccessful() )
            {
                throw new RuntimeException( "Not consistent database in " + storeDir );
            }
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( "Failed to run CC on " + storeDir, e );
        }
    }
}
