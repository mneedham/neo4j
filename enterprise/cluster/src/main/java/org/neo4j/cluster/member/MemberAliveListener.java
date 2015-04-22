package org.neo4j.cluster.member;

import org.neo4j.cluster.InstanceId;

/**
 * A HighAvailabilityListener is listening for events from elections and availability state.
 * <p/>
 * These are invoked by translating atomic broadcast messages to methods on this interface.
 */

public interface MemberAliveListener
{

    void memberIsFailed( InstanceId instanceId );

    void memberIsAlive( InstanceId instanceId );
}

