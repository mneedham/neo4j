package org.neo4j.kernel.ha.factory;

import java.util.ArrayList;
import java.util.List;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.HazelcastLifecycle;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.ha.MyElection;
import org.neo4j.helpers.Listeners;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;


public class ElectionOutcomeWhiteboard extends LifecycleAdapter implements ClusterMemberEvents
{
    private List<ClusterMemberListener> listeners = new ArrayList<>();
    public static final String MAP_ROLES = "ROLES";

    public ElectionOutcomeWhiteboard( final HazelcastLifecycle hazelcastLifecycle, final MyElection election )
    {
        hazelcastLifecycle.addStartupListener( new HazelcastLifecycle.StartupListener()
        {
            @Override
            public void hazelcastStarted( final HazelcastInstance hazelcastInstance )
            {
                final ITopic<InstanceId> topic = hazelcastInstance.getTopic( MAP_ROLES );
                topic.addMessageListener( new MessageListener<InstanceId>()
                {
                    @Override
                    public void onMessage( Message<InstanceId> message )
                    {
                        InstanceId instanceId = message.getMessageObject();
                        System.out.println( "*&*&*& notify masterIsElected " + instanceId );
                        notifyElectionOutcome( instanceId );
                    }
                } );

                final IAtomicReference<InstanceId> coordinator = hazelcastInstance.getAtomicReference( "coordinator" );
                notifyElectionOutcome( coordinator.get() );

                election.addOutcomeListener( new ClusterMemberListener.Adapter()
                {
                    @Override
                    public void coordinatorIsElected( InstanceId coordinatorId )
                    {
                        coordinator.set( coordinatorId );
                        topic.publish( coordinatorId );
                    }

                } );
            }
        } );
    }

    private void notifyElectionOutcome( final InstanceId instanceId )
    {
        if ( instanceId != null )
        {
            Listeners.notifyListeners(listeners, new Listeners.Notification<ClusterMemberListener>()
            {
                @Override public void notify( ClusterMemberListener listener )
                {
                    listener.coordinatorIsElected( instanceId );
                }
            });
        }
    }

    @Override
    public void start() throws Throwable
    {
    }

    public void addClusterMemberListener( ClusterMemberListener toAdd )
    {
        listeners.add( toAdd );
    }

    @Override public void removeClusterMemberListener( ClusterMemberListener listener )
    {

    }
}
