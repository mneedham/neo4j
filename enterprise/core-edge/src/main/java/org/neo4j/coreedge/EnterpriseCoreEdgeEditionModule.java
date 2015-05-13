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
package org.neo4j.coreedge;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.InitialMembershipEvent;
import com.hazelcast.core.InitialMembershipListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import org.jboss.netty.logging.InternalLoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.lang.reflect.Proxy;
import java.util.List;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.Clusters;
import org.neo4j.cluster.logging.NettyLoggerFactory;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.BranchedDataMigrator;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.LabelTokenCreatorModeSwitcher;
import org.neo4j.kernel.ha.LastUpdateTime;
import org.neo4j.kernel.ha.PropertyKeyCreatorModeSwitcher;
import org.neo4j.kernel.ha.RelationshipTypeCreatorModeSwitcher;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.UpdateSource;
import org.neo4j.kernel.ha.com.slave.InvalidEpochExceptionHandler;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.CoreEdgeTransactionCommitProcess;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoreInjectedTransactionValidator;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Enterprise edition that provides a core/edge cluster.
 */
public class EnterpriseCoreEdgeEditionModule extends EditionModule
{
    private final boolean isCoreServer;

    public EnterpriseCoreEdgeEditionModule( final PlatformModule platformModule )
    {
        final LifeSupport life = platformModule.getLife();
        final Config config = platformModule.getConfig();

        System.out.println(config);

        final Dependencies dependencies = platformModule.getDependencies();
        final LogService logging = platformModule.getLogging();
        this.isCoreServer = config.get( ClusterSettings.server_type ).equalsIgnoreCase( "CORE" );
        InstanceId serverId = config.get( ClusterSettings.server_id );
        this.transactionStartTimeout = config.get( HaSettings.state_switch_timeout );

        // Set Netty logger
        InternalLoggerFactory.setDefaultFactory( new NettyLoggerFactory( logging.getInternalLogProvider() ) );

        life.add( new BranchedDataMigrator( platformModule.getStoreDir() ) );
        DelegateInvocationHandler<Master> masterDelegateInvocationHandler = new DelegateInvocationHandler<>( Master
                .class );

        RequestContextFactory requestContextFactory = dependencies.satisfyDependency( new RequestContextFactory(
                serverId.toIntegerIndex(),
                dependencies ) );

        if ( isCoreServer )
        {
            HazelcastLifecycle hazelcast = new HazelcastLifecycle( config );
            life.add( hazelcast );
        }
        else
        {
            HazelcastClientLifeCycle hazelcastClient = new HazelcastClientLifeCycle(config) ;
            life.add(hazelcastClient);

        }

        // TODO: log events in the HC cluster
        this.idGeneratorFactory = dependencies.satisfyDependency(
                createIdGeneratorFactory( masterDelegateInvocationHandler, logging.getInternalLogProvider(),
                        requestContextFactory ) );

        LastUpdateTime lastUpdateTime = new LastUpdateTime();

        UpdatePuller updatePuller = dependencies.satisfyDependency( life.add(
                new UpdatePuller( new FakeHighAvailabilityMemberStateMachine(), requestContextFactory,
                        anyCoreMachine(),
                        lastUpdateTime,
                        logging.getInternalLogProvider(), serverId, new InvalidEpochExceptionHandler()
                {
                    @Override
                    public void handle()
                    {
                        // Do nothing
                    }
                } ) ) );
//        dependencies.satisfyDependency( life.add( new UpdatePullerClient( config.get( HaSettings.pull_interval ),
//                platformModule.getJobScheduler(), logging.getInternalLogProvider(), updatePuller, platformModule
//                .getAvailabilityGuard() ) ) );
//        dependencies.satisfyDependency( life.add( new UpdatePullingTransactionObligationFulfiller(
//                updatePuller, memberStateMachine, serverId, dependencies ) ) );

        this.schemaWriteGuard = new SchemaWriteGuard()
        {
            @Override
            public void assertSchemaWritesAllowed() throws InvalidTransactionTypeKernelException
            {
                if ( !isCoreSever() )
                {
                    throw new InvalidTransactionTypeKernelException(
                            "Modifying database schema can only be done on a core server, this is an edge server." );
                }
            }
        };

        this.propertyKeyTokenHolder = new PropertyKeyTokenHolder( null );
        this.labelTokenHolder = new LabelTokenHolder( null );
        this.relationshipTypeTokenHolder = new RelationshipTypeTokenHolder( null );
        this.commitProcessFactory = createCommitProcessFactory();
    }

    private boolean isCoreSever()
    {
        return this.isCoreServer;
    }

    private boolean isEdgeServer()
    {
        return !this.isCoreServer;
    }

    private UpdateSource anyCoreMachine()
    {
        return new UpdateSource()
        {
            @Override
            public Response<Void> pullUpdates( RequestContext context )
            {
                throw new NotImplementedException();
            }
        };
    }

    protected TokenCreator createRelationshipTypeCreator( Config config,
            HighAvailabilityMemberStateMachine memberStateMachine,
            DelegateInvocationHandler<Master> masterDelegateInvocationHandler,
            RequestContextFactory requestContextFactory,
            Supplier<KernelAPI> kernelProvider )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            DelegateInvocationHandler<TokenCreator> relationshipTypeCreatorDelegate =
                    new DelegateInvocationHandler<>( TokenCreator.class );
            TokenCreator relationshipTypeCreator =
                    (TokenCreator) Proxy.newProxyInstance( TokenCreator.class.getClassLoader(),
                            new Class[]{TokenCreator.class}, relationshipTypeCreatorDelegate );

            new RelationshipTypeCreatorModeSwitcher( memberStateMachine, relationshipTypeCreatorDelegate,
                    masterDelegateInvocationHandler, requestContextFactory, kernelProvider, idGeneratorFactory );

            return relationshipTypeCreator;
        }
    }


    protected TokenCreator createLabelIdCreator( Config config, HighAvailabilityMemberStateMachine memberStateMachine,
            DelegateInvocationHandler<Master> masterDelegateInvocationHandler,
            RequestContextFactory requestContextFactory,
            Supplier<KernelAPI> kernelProvider )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            DelegateInvocationHandler<TokenCreator> labelIdCreatorDelegate =
                    new DelegateInvocationHandler<>( TokenCreator.class );
            TokenCreator labelIdCreator =
                    (TokenCreator) Proxy.newProxyInstance( TokenCreator.class.getClassLoader(),
                            new Class[]{TokenCreator.class}, labelIdCreatorDelegate );
            new LabelTokenCreatorModeSwitcher( memberStateMachine, labelIdCreatorDelegate,
                    masterDelegateInvocationHandler, requestContextFactory, kernelProvider, idGeneratorFactory );
            return labelIdCreator;
        }
    }


    protected TokenCreator createPropertyKeyCreator( Config config,
            HighAvailabilityMemberStateMachine memberStateMachine,
            DelegateInvocationHandler<Master> masterDelegateInvocationHandler,
            RequestContextFactory requestContextFactory,
            Supplier<KernelAPI> kernelProvider )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            DelegateInvocationHandler<TokenCreator> propertyKeyCreatorDelegate =
                    new DelegateInvocationHandler<>( TokenCreator.class );
            TokenCreator propertyTokenCreator =
                    (TokenCreator) Proxy.newProxyInstance( TokenCreator.class.getClassLoader(),
                            new Class[]{TokenCreator.class}, propertyKeyCreatorDelegate );
            new PropertyKeyCreatorModeSwitcher( memberStateMachine, propertyKeyCreatorDelegate,
                    masterDelegateInvocationHandler, requestContextFactory, kernelProvider, idGeneratorFactory );
            return propertyTokenCreator;
        }
    }

    protected CommitProcessFactory createCommitProcessFactory()
    {


        return new CommitProcessFactory()
        {
            @Override
            public TransactionCommitProcess create( LogicalTransactionStore logicalTransactionStore,
                    KernelHealth kernelHealth, NeoStore neoStore,
                    TransactionRepresentationStoreApplier storeApplier,
                    NeoStoreInjectedTransactionValidator txValidator,
                    IndexUpdatesValidator indexUpdatesValidator,
                    TransactionApplicationMode mode, Config config )
            {
                return new CoreEdgeTransactionCommitProcess();
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

    private static class FakeHighAvailabilityMemberStateMachine extends HighAvailabilityMemberStateMachine
    {
        public FakeHighAvailabilityMemberStateMachine()
        {
            super( null, null, null, null, null, NullLogProvider.getInstance() );
        }

        @Override
        public synchronized void start()
        {
            doNothing();
        }

        @Override
        public void stop()
        {
            doNothing();
        }

        private void doNothing()
        {
            // Like the method says, do nothing.
        }
    }
}
