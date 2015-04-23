package org.neo4j.kernel.ha.factory;

import java.util.Collections;
import java.util.Set;

import org.neo4j.cluster.ClusterManagement;
import org.neo4j.cluster.ClusterManagementEventListener;
import org.neo4j.cluster.InstanceId;
import org.neo4j.helpers.collection.IteratorUtil;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class HazelcastClusterManagement implements ClusterManagement
{

    @Override
    public Set<InstanceId> getFailed()
    {
        return Collections.emptySet();
    }

    @Override
    public void setFailed( Set<InstanceId> failures )
    {

    }

    @Override
    public Set<InstanceId> getMembers()
    {
        return asSet( new InstanceId( 1 ), new InstanceId( 2 ), new InstanceId( 3 ) );
    }

    @Override
    public Set<InstanceId> getAlive()
    {
        return asSet( new InstanceId( 1 ), new InstanceId( 2 ), new InstanceId( 3 ) );
    }

    @Override
    public void addListener( ClusterManagementEventListener clusterManagementEventListener )
    {

    }

    @Override
    public void fail( InstanceId failingInstance )
    {

    }
}
