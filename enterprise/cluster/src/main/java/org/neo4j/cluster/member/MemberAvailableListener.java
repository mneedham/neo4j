package org.neo4j.cluster.member;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.kernel.impl.store.StoreId;

public interface MemberAvailableListener
{

    /**
     * Called when a member announces that it is available to play a particular role, e.g. master or slave.
     * After this it can be assumed that the member is ready to consume messages related to that role.
     *
     * @param role
     * @param availableId the role connection information for the new role holder
     * @param atUri the URI at which the instance is available at
     * @param storeId the identifier of a store that became available
     */
    void memberIsAvailable( String role, InstanceId availableId, URI atUri, StoreId storeId );

    /**
     * Called when a member is no longer available for fulfilling a particular role.
     *
     * @param role The role for which the member is unavailable
     * @param unavailableId The id of the member which became unavailable for that role
     */
    void memberIsUnavailable( String role, InstanceId unavailableId );
}
