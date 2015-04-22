package org.neo4j.cluster.member;

public interface ClusterMemberElectionEvents {
    void addClusterMemberElectionListener( MemberElectionListener listener );

    void removeClusterMemberElectionListener( MemberElectionListener listener );
}

