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

import java.io.Serializable;
import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.kernel.impl.store.StoreId;

public class ClusterMemberAvailabilityState implements Serializable
{
    private final InstanceId instanceId;
    private final String role;
    private final URI atUri;
    private final StoreId storeId;
    private final boolean available;

    public ClusterMemberAvailabilityState( InstanceId instanceId, String role, URI atUri, StoreId storeId, boolean
            available )
    {
        this.instanceId = instanceId;
        this.role = role;
        this.atUri = atUri;
        this.storeId = storeId;
        this.available = available;
    }

    public InstanceId getInstanceId()
    {
        return instanceId;
    }

    public String getRole()
    {
        return role;
    }

    public URI getAtUri()
    {
        return atUri;
    }

    public StoreId getStoreId()
    {
        return storeId;
    }

    public boolean isAvailable()
    {
        return available;
    }

    @Override
    public String toString()
    {
        return "ClusterMemberAvailabilityState{" +
               "instanceId=" + instanceId +
               ", role='" + role + '\'' +
               ", atUri=" + atUri +
               ", storeId=" + storeId +
               ", available=" + available +
               '}';
    }
}
