package org.neo4j.cluster.member;

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
