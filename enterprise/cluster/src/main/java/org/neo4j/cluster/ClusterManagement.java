package org.neo4j.cluster;

import java.util.Set;

public interface ClusterManagement
{

    Set<InstanceId> getFailed();
    void setFailed(Set<InstanceId> failures);

    Set<InstanceId> getMembers();

    Set<InstanceId> getAlive();

    void addListener( ClusterManagementEventListener clusterManagementEventListener);

    void fail( InstanceId failingInstance );
}
