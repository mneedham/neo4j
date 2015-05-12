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
package org.neo4j.ha;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.hazelcast.core.Member;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.helpers.Listeners;

import static org.neo4j.cluster.client.HazelcastLifecycle.instanceIdFor;

public class MyElection
{
    private List<ClusterMemberListener> listeners = new ArrayList<>();

    public void triggerElection( Set<Member> members )
    {
        SortedSet<Member> sortedMembers = new TreeSet<Member>( new Comparator<Member>()
        {
            @Override
            public int compare( Member o1, Member o2 )
            {
                return instanceIdFor( o1 ).compareTo( instanceIdFor( o2 ) );
            }
        } );
        sortedMembers.addAll( members );


        if ( sortedMembers.isEmpty() )
        {
            return;
        }

        if ( !sortedMembers.first().localMember() )
        {
            return;
        }

        Member master = sortedMembers.first();
        final InstanceId instanceId = instanceIdFor(master);

        Listeners.notifyListeners( listeners,
                new Listeners.Notification<ClusterMemberListener>()
                {
                    @Override
                    public void notify( ClusterMemberListener listener )
                    {
                        listener.coordinatorIsElected( instanceId );
                    }
                } );
    }

    public void addOutcomeListener( ClusterMemberListener memberListener )
    {
        listeners.add( memberListener );
    }
}
