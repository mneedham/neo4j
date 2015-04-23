package org.neo4j.cluster;

import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.logging.LogProvider;

public class ReelectionListener implements ClusterManagementEventListener
{
    public ReelectionListener( Election election, LogProvider logProvider )
    {
    }
}
