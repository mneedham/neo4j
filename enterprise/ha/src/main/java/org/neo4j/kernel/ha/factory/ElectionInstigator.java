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
package org.neo4j.kernel.ha.factory;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;

import org.neo4j.cluster.client.HazelcastLifecycle;
import org.neo4j.ha.MyElection;

public class ElectionInstigator
{
    private final HazelcastLifecycle hazelcast;
    private final MyElection election;

    public ElectionInstigator( HazelcastLifecycle hazelcast, MyElection election )
    {
        this.hazelcast = hazelcast;
        this.hazelcast.addStartupListener( new HazelcastLifecycle.StartupListener()
        {
            @Override
            public void hazelcastStarted( HazelcastInstance hazelcastInstance )
            {
                hazelcastInstance.getCluster().addMembershipListener( new InitialMembershipListener()
                {
                    private final MyElection myElection = ElectionInstigator.this.election;

                    @Override
                    public void memberAdded( final MembershipEvent membershipEvent )
                    {
                        myElection.triggerElection( membershipEvent.getMembers() );
                    }

                    @Override
                    public void memberRemoved( final MembershipEvent membershipEvent )
                    {
                        myElection.triggerElection( membershipEvent.getMembers() );
                    }

                    @Override
                    public void memberAttributeChanged( MemberAttributeEvent memberAttributeEvent )
                    {

                    }

                    @Override
                    public void init( final InitialMembershipEvent event )
                    {
                        myElection.triggerElection( event.getMembers() );
                    }
                } );
            }
        } );
        this.election = election;
    }

    public void forceElections()
    {
        election.triggerElection( hazelcast.members() );
    }
}
