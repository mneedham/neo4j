package org.neo4j.cluster.member;

public interface ClusterMemberAvailableEvents {
    void addClusterMemberAvailableListener( MemberAvailableListener listener );

    void removeClusterMemberAvailableListener( MemberAvailableListener listener );
}
