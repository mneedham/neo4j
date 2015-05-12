/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha.factory;

import org.jboss.netty.logging.InternalLoggerFactory;

import java.lang.reflect.Proxy;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.client.HazelcastLifecycle;
import org.neo4j.cluster.logging.NettyLoggerFactory;
import org.neo4j.ha.MyElection;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.BranchedDataMigrator;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.LastUpdateTime;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.UpdatePullerClient;
import org.neo4j.kernel.ha.UpdatePullingTransactionObligationFulfiller;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.member.HAClusterMembers;
import org.neo4j.kernel.ha.cluster.member.HazelcastClusterMembers;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.slave.InvalidEpochExceptionHandler;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.ha.management.ClusterDatabaseInfoProvider;
import org.neo4j.kernel.ha.management.HighlyAvailableKernelData;
import org.neo4j.kernel.ha.transaction.OnDiskLastTxIdGetter;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Enterprise edition.
 */
public class EnterpriseCoreEdgeEditionModule
        extends EditionModule
{
    public HighAvailabilityMemberStateMachine memberStateMachine;
    public ClusterMembers members;

    public EnterpriseCoreEdgeEditionModule( final PlatformModule platformModule )
    {
        final LifeSupport life = platformModule.getLife();
        final Config config = platformModule.getConfig();
        final Dependencies dependencies = platformModule.getDependencies();
        final LogService logging = platformModule.getLogging();

        // Set Netty logger
        InternalLoggerFactory.setDefaultFactory( new NettyLoggerFactory( logging.getInternalLogProvider() ) );

        life.add( new BranchedDataMigrator( platformModule.getStoreDir() ) );
        DelegateInvocationHandler<Master> masterDelegateInvocationHandler = new DelegateInvocationHandler<>( Master
                .class );
        Master master = (Master) Proxy.newProxyInstance( Master.class.getClassLoader(), new Class[]{Master.class},
                masterDelegateInvocationHandler );
        InstanceId serverId = config.get( ClusterSettings.server_id );
        RequestContextFactory requestContextFactory = dependencies.satisfyDependency( new RequestContextFactory(
                serverId.toIntegerIndex(),
                dependencies ) );

        transactionStartTimeout = config.get( HaSettings.state_switch_timeout );


        MyElection election = new MyElection();

        HazelcastLifecycle hazelcast = new HazelcastLifecycle( ClusterClient.adapt( config ) );
        life.add( hazelcast );
        ElectionOutcomeWhiteboard electionOutcomeWhiteboard =
                life.add( new ElectionOutcomeWhiteboard( hazelcast, election ) );

        final ElectionInstigator electionInstigator = new ElectionInstigator( hazelcast, election );


        MemberAvailabilityWhiteboard memberAvailabilityWhiteboard =
                life.add( new MemberAvailabilityWhiteboard( hazelcast ) );
        members = dependencies.satisfyDependency( new HazelcastClusterMembers( new ClusterEventsAdapter( hazelcast,
                logging.getInternalLogProvider() ),
                electionOutcomeWhiteboard, memberAvailabilityWhiteboard,
                config.get( ClusterSettings.server_id ) ) );


        // TODO: log events in the HC cluster
        idGeneratorFactory = dependencies.satisfyDependency(
                createIdGeneratorFactory( masterDelegateInvocationHandler, logging.getInternalLogProvider(),
                        requestContextFactory ) );


        // TODO: Something to resolve one or more of the core servers here


        // TODO: manage this properly.
        LastUpdateTime lastUpdateTime = new LastUpdateTime();

        UpdatePuller updatePuller = dependencies.satisfyDependency( life.add(
                new UpdatePuller( memberStateMachine, requestContextFactory, master, lastUpdateTime,
                        logging.getInternalLogProvider(), serverId,
                        getInvalidEpochExceptionHandler( electionInstigator ) ) ) );
        dependencies.satisfyDependency( life.add( new UpdatePullerClient( config.get( HaSettings.pull_interval ),
                platformModule.getJobScheduler(), logging.getInternalLogProvider(), updatePuller, platformModule
                .getAvailabilityGuard() ) ) );
        dependencies.satisfyDependency( life.add( new UpdatePullingTransactionObligationFulfiller(
                updatePuller, memberStateMachine, serverId, dependencies ) ) );

        schemaWriteGuard = new SchemaWriteGuard()
        {
            @Override
            public void assertSchemaWritesAllowed() throws InvalidTransactionTypeKernelException
            {
                if ( !memberStateMachine.isMaster() )
                {
                    throw new InvalidTransactionTypeKernelException(
                            "Modifying the database schema can only be done on the master server, " +
                            "this server is a slave. Please issue schema modification commands directly to " +
                            "the master."
                    );
                }
            }
        };

        life.add( dependencies.satisfyDependency(createKernelData( config, platformModule.getGraphDatabaseFacade(),
                null, lastUpdateTime ) ));
    }

    protected KernelData createKernelData( Config config, GraphDatabaseAPI graphDb, HAClusterMembers members, LastUpdateTime lastUpdateTime)
    {
        OnDiskLastTxIdGetter txIdGetter = new OnDiskLastTxIdGetter( graphDb );
        ClusterDatabaseInfoProvider databaseInfo = new ClusterDatabaseInfoProvider(
                members, txIdGetter, lastUpdateTime );
        return new HighlyAvailableKernelData( graphDb, members, databaseInfo, config );
    }


    private InvalidEpochExceptionHandler getInvalidEpochExceptionHandler( final ElectionInstigator electionInstigator )
    {
        return new InvalidEpochExceptionHandler()
        {
            @Override
            public void handle()
            {
                electionInstigator.forceElections();
            }
        };
    }

    protected IdGeneratorFactory createIdGeneratorFactory(
            DelegateInvocationHandler<Master> masterDelegateInvocationHandler, LogProvider logging,
            RequestContextFactory requestContextFactory )
    {
        idGeneratorFactory = new HaIdGeneratorFactory( masterDelegateInvocationHandler, logging,
                requestContextFactory );

        /*
         * We don't really switch to master here. We just need to initialize the idGenerator so the initial store
         * can be started (if required). In any case, the rest of the database is in pending state, so nothing will
         * happen until events start arriving and that will set us to the proper state anyway.
         */
        ((HaIdGeneratorFactory) idGeneratorFactory).switchToMaster();

        return idGeneratorFactory;
    }
}
