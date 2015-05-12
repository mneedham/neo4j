package org.neo4j.kernel.ha.cluster.member;

/**
 * Created by markneedham on 11/05/15.
 */
public interface ClusterMembers
{
    Iterable<ClusterMember> getMembers();

    ClusterMember getSelf();

    void waitForEvent( long timeout ) throws InterruptedException;
}
