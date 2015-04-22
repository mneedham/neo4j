package org.neo4j.cluster.member;

public interface ClusterMemberAliveEvents
{
    void addClusterMemberAliveListener( MemberAliveListener listener );

    void removeClusterMemberAliveListener( MemberAliveListener listener );
}

