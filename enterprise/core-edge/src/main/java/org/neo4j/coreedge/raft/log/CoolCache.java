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
package org.neo4j.coreedge.raft.log;

import java.io.IOException;

import org.neo4j.cursor.IOCursor;
import org.neo4j.helpers.collection.LruCache;

import static java.lang.String.format;

public class CoolCache
{
    private final LruCache<Long, RaftLogEntry> cache;

    public CoolCache( int maxSize )
    {
        this.cache = new LruCache<>( "RaftLogEntriesCache", maxSize );
    }

    public RaftLogEntry get( IOCursor<RaftLogAppendRecord> cursor, long logIndex ) throws RaftStorageException
    {
        RaftLogEntry entry;
        if ( (entry = cache.get( logIndex )) != null )
        {
            return entry;
        }
        else
        {
            try
            {
                while ( cursor.next() )
                {
                    RaftLogAppendRecord raftLogAppendRecord = cursor.get();

                    cache.put( raftLogAppendRecord.getLogIndex(), raftLogAppendRecord.getLogEntry() );

                    if ( raftLogAppendRecord.getLogIndex() == logIndex )
                    {
                        return raftLogAppendRecord.getLogEntry();
                    }
                    else if ( raftLogAppendRecord.getLogIndex() > logIndex )
                    {
                        throw new IllegalStateException( format( "Asked for index %d but got up to %d without " +
                                "finding it.", logIndex, raftLogAppendRecord.getLogIndex() ) );
                    }
                }
            }
            catch ( IOException e )
            {
                throw new RaftStorageException( e );
            }
        }
        return null;
    }
}
