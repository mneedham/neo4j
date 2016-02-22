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

import org.junit.Test;

import org.neo4j.cursor.IOCursor;

import static java.lang.String.format;

public class CoolCacheTest
{
    @Test
    public void shouldStoreLast10Entries() throws Exception
    {
        // given
        CoolCache cache = new CoolCache(10);

        // when
        IOCursor<RaftLogAppendRecord> cursor = cursor(  );
//        cache.populate( cursor );

        // then
//        cache.get( entryStore.getEntriesFrom( logIndex ), 5L);
    }

    private IOCursor<RaftLogAppendRecord> cursor( final RaftLogEntry... entries )
    {
        return new IOCursor<RaftLogAppendRecord>()
        {
            private long pos = 0;

            @Override
            public RaftLogAppendRecord get()
            {
                return new RaftLogAppendRecord( pos, entries[(int)pos++]);
            }

            @Override
            public boolean next() throws IOException
            {
                return pos < entries.length;
            }

            @Override
            public void close() throws IOException
            {// nothing to do
            }
        };
    }
}