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
package org.neo4j.coreedge.discovery;

import java.util.HashSet;
import java.util.Set;

import com.hazelcast.core.Member;

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;

import static org.neo4j.coreedge.discovery.HazelcastServerLifecycle.RAFT_SERVER;
import static org.neo4j.coreedge.discovery.HazelcastServerLifecycle.TRANSACTION_SERVER;

public class HazelcastClusterTopology implements ClusterTopology
{
    public static final String EDGE_SERVERS = "edge-servers";
    private final Set<Member> hazelcastMembers;

    public HazelcastClusterTopology( Set<Member> hazelcastMembers )
    {
        this.hazelcastMembers = hazelcastMembers;
    }

    @Override
    public boolean bootstrappable()
    {
        Member firstMember = hazelcastMembers.iterator().next();
        return firstMember.localMember();
    }

    @Override
    public int getNumberOfCoreServers()
    {
        return hazelcastMembers.size();
    }

    @Override
    public Set<CoreMember> getMembers()
    {
        return toCoreMembers( hazelcastMembers );
    }

    private Set<CoreMember> toCoreMembers( Set<Member> members )
    {
        HashSet<CoreMember> coreMembers = new HashSet<>();

        for ( Member member : members )
        {
            coreMembers.add( new CoreMember(
                    new AdvertisedSocketAddress( member.getStringAttribute( TRANSACTION_SERVER ) ),
                    new AdvertisedSocketAddress( member.getStringAttribute( RAFT_SERVER ) )
            ));
        }

        return coreMembers;
    }

    @Override
    public AdvertisedSocketAddress firstTransactionServer()
    {
        Member member = hazelcastMembers.iterator().next();
        return new AdvertisedSocketAddress( member.getStringAttribute( TRANSACTION_SERVER ) );
    }
}
