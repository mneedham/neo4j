package org.neo4j.cluster.member;

import org.neo4j.cluster.InstanceId;


public interface MemberElectionListener
{
    /**
     * Called when new coordinator has been elected.
     *
     * @param coordinatorId the Id of the coordinator
     */
    void coordinatorIsElected( InstanceId coordinatorId );
}
