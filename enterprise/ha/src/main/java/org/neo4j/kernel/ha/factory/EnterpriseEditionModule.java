/**
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

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.jboss.netty.logging.InternalLoggerFactory;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.logging.NettyLoggerFactory;
import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentialsProvider;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker;
import org.neo4j.function.Factory;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.BranchedDataMigrator;
import org.neo4j.kernel.ha.CommitProcessSwitcher;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighAvailabilityLogger;
import org.neo4j.kernel.ha.HighAvailabilityMemberInfoProvider;
import org.neo4j.kernel.ha.LabelTokenCreatorModeSwitcher;
import org.neo4j.kernel.ha.LastUpdateTime;
import org.neo4j.kernel.ha.PropertyKeyCreatorModeSwitcher;
import org.neo4j.kernel.ha.RelationshipTypeCreatorModeSwitcher;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.UpdatePullerClient;
import org.neo4j.kernel.ha.UpdatePullingTransactionObligationFulfiller;
import org.neo4j.kernel.ha.cluster.DefaultElectionCredentialsProvider;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberContext;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.cluster.SimpleHighAvailabilityMemberContext;
import org.neo4j.kernel.ha.cluster.SwitchToMaster;
import org.neo4j.kernel.ha.cluster.SwitchToSlave;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.member.HighAvailabilitySlaves;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.DefaultSlaveFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.ha.com.master.SlaveFactory;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.ha.com.slave.InvalidEpochExceptionHandler;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.ha.lock.LockManagerModeSwitcher;
import org.neo4j.kernel.ha.management.ClusterDatabaseInfoProvider;
import org.neo4j.kernel.ha.management.HighlyAvailableKernelData;
import org.neo4j.kernel.ha.transaction.CommitPusher;
import org.neo4j.kernel.ha.transaction.OnDiskLastTxIdGetter;
import org.neo4j.kernel.ha.transaction.TransactionPropagator;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.RemoveOrphanConstraintIndexesOnStartup;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.storemigration.UpgradeConfiguration;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByDatabaseModeException;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoreInjectedTransactionValidator;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Enterprise edition.
 */
public class EnterpriseEditionModule
        extends EditionModule
{
    public HighAvailabilityMemberStateMachine memberStateMachine;
    public ClusterMembers members;

    public EnterpriseEditionModule( final PlatformModule platformModule )
    {
        final LifeSupport life = platformModule.life;
        final Config config = platformModule.config;
        final Dependencies dependencies = platformModule.dependencies;
        final LogService logging = platformModule.logging;
        final Monitors monitors = platformModule.monitors;

        // Set Netty logger
        InternalLoggerFactory.setDefaultFactory( new NettyLoggerFactory( logging.getInternalLogProvider() ) );

        life.add( new BranchedDataMigrator( platformModule.storeDir ) );
        DelegateInvocationHandler<Master> masterDelegateInvocationHandler = new DelegateInvocationHandler<>( Master
                .class );
        Master master = (Master) Proxy.newProxyInstance( Master.class.getClassLoader(), new Class[]{Master.class},
                masterDelegateInvocationHandler );
        InstanceId serverId = config.get( ClusterSettings.server_id );
        RequestContextFactory requestContextFactory = dependencies.satisfyDependency( new RequestContextFactory(
                serverId.toIntegerIndex(),
                dependencies ) );

        TransactionCommittingResponseUnpacker responseUnpacker = dependencies.satisfyDependency(
                new TransactionCommittingResponseUnpacker( dependencies ) );

        Supplier<KernelAPI> kernelProvider = dependencies.provideDependency( KernelAPI.class );

        transactionStartTimeout = config.get( HaSettings.state_switch_timeout );

        DelegateInvocationHandler<ClusterMemberEvents> clusterEventsDelegateInvocationHandler =
                new DelegateInvocationHandler<>( ClusterMemberEvents.class );
        DelegateInvocationHandler<HighAvailabilityMemberContext> memberContextDelegateInvocationHandler =
                new DelegateInvocationHandler<>( HighAvailabilityMemberContext.class );
        DelegateInvocationHandler<ClusterMemberAvailability> clusterMemberAvailabilityDelegateInvocationHandler =
                new DelegateInvocationHandler<>( ClusterMemberAvailability.class );

        ClusterMemberEvents clusterEvents = dependencies.satisfyDependency(
                (ClusterMemberEvents) Proxy.newProxyInstance(
                        ClusterMemberEvents.class.getClassLoader(),
                        new Class[]{ClusterMemberEvents.class, Lifecycle.class},
                        clusterEventsDelegateInvocationHandler ) );

        HighAvailabilityMemberContext memberContext = (HighAvailabilityMemberContext) Proxy.newProxyInstance(
                HighAvailabilityMemberContext.class.getClassLoader(),
                new Class[]{HighAvailabilityMemberContext.class}, memberContextDelegateInvocationHandler );
        ClusterMemberAvailability clusterMemberAvailability = dependencies.satisfyDependency(
                (ClusterMemberAvailability) Proxy.newProxyInstance(
                ClusterMemberAvailability.class.getClassLoader(),
                new Class[]{ClusterMemberAvailability.class}, clusterMemberAvailabilityDelegateInvocationHandler ) );

        // TODO There's a cyclical dependency here that should be fixed
        final AtomicReference<HighAvailabilityMemberStateMachine> electionProviderRef = new AtomicReference<>(  );
        ElectionCredentialsProvider electionCredentialsProvider = config.get( HaSettings.slave_only ) ?
                new NotElectableElectionCredentialsProvider() :
                new DefaultElectionCredentialsProvider(
                        config.get( ClusterSettings.server_id ),
                        new OnDiskLastTxIdGetter( platformModule.graphDatabaseFacade ),
                        new HighAvailabilityMemberInfoProvider()
                        {
                            @Override
                            public HighAvailabilityMemberState
                            getHighAvailabilityMemberState()
                            {
                                return electionProviderRef.get().getCurrentState();
                            }
                        }
                );


        HazelcastInstance instance = createHazelcastInstance( config );

        HighAvailabilityMemberContext localMemberContext = new SimpleHighAvailabilityMemberContext( new InstanceId( Integer.valueOf( instance.getName())), config.get( HaSettings.slave_only ) );

        memberContextDelegateInvocationHandler.setDelegate( localMemberContext );

        memberStateMachine = new HighAvailabilityMemberStateMachine(
                memberContext, platformModule.availabilityGuard, members,
                clusterEvents,
                null, logging.getInternalLogProvider() );

        HighAvailabilityLogger highAvailabilityLogger = new HighAvailabilityLogger( logging.getUserLogProvider(),
                config.get( ClusterSettings.server_id ) );
        platformModule.availabilityGuard.addListener( highAvailabilityLogger );
//        clusterEvents.addClusterMemberListener( highAvailabilityLogger );

        idGeneratorFactory = dependencies.satisfyDependency(createIdGeneratorFactory( masterDelegateInvocationHandler, logging.getInternalLogProvider(), requestContextFactory ));

        // TODO There's a cyclical dependency here that should be fixed
        final AtomicReference<HighAvailabilityModeSwitcher> exceptionHandlerRef = new AtomicReference<>(  );
        InvalidEpochExceptionHandler invalidEpochHandler = new InvalidEpochExceptionHandler()
        {
            @Override
            public void handle()
            {
                exceptionHandlerRef.get().forceElections();
            }
        };

        MasterClientResolver masterClientResolver = new MasterClientResolver( logging.getInternalLogProvider(), responseUnpacker,
                invalidEpochHandler,
                config.get( HaSettings.read_timeout ).intValue(),
                config.get( HaSettings.lock_read_timeout ).intValue(),
                config.get( HaSettings.max_concurrent_channels_per_slave ),
                config.get( HaSettings.com_chunk_size ).intValue() );

        SwitchToSlave switchToSlaveInstance = new SwitchToSlave( logging, config, dependencies,
                (HaIdGeneratorFactory) idGeneratorFactory,
                masterDelegateInvocationHandler, clusterMemberAvailability,
                requestContextFactory, platformModule.kernelExtensions.listFactories(), masterClientResolver,
                monitors.newMonitor( ByteCounterMonitor.class, SlaveServer.class ),
                monitors.newMonitor( RequestMonitor.class, SlaveServer.class ),
                monitors.newMonitor( SwitchToSlave.Monitor.class ),
                monitors.newMonitor( StoreCopyClient.Monitor.class ) );

        SwitchToMaster switchToMasterInstance = new SwitchToMaster( logging, platformModule.graphDatabaseFacade,
                (HaIdGeneratorFactory) idGeneratorFactory, config, dependencies.provideDependency( SlaveFactory.class ),
                masterDelegateInvocationHandler, clusterMemberAvailability, platformModule.dataSourceManager,
                monitors.newMonitor( ByteCounterMonitor.class, MasterServer.class ),
                monitors.newMonitor( RequestMonitor.class, MasterServer.class ),
                monitors.newMonitor( MasterImpl.Monitor.class, MasterImpl.class ) );

        final HighAvailabilityModeSwitcher highAvailabilityModeSwitcher = new HighAvailabilityModeSwitcher(
                switchToSlaveInstance, switchToMasterInstance,
                instance, clusterMemberAvailability, instance, new Supplier<StoreId>()
        {
            @Override
            public StoreId get()
            {
                return dependencies.resolveDependency( NeoStoreDataSource.class ).getStoreId();
            }
        }, config.get( ClusterSettings.server_id ),
                logging );
        exceptionHandlerRef.set( highAvailabilityModeSwitcher );

//        clusterClient.addBindingListener( highAvailabilityModeSwitcher );
//        memberStateMachine.addHighAvailabilityMemberListener( highAvailabilityModeSwitcher );

        /*
         * We always need the mode switcher and we need it to restart on switchover.
         */


        life.add( requestContextFactory );

        life.add( responseUnpacker );

        LastUpdateTime lastUpdateTime = new LastUpdateTime();

        UpdatePuller updatePuller = dependencies.satisfyDependency( life.add(
                new UpdatePuller( memberStateMachine, requestContextFactory, master, lastUpdateTime,
                        logging.getInternalLogProvider(), serverId, invalidEpochHandler ) ) );
        dependencies.satisfyDependency( life.add( new UpdatePullerClient( config.get( HaSettings.pull_interval ),
                platformModule.jobScheduler, logging.getInternalLogProvider(), updatePuller, platformModule
                .availabilityGuard ) ) );
        dependencies.satisfyDependency( life.add( new UpdatePullingTransactionObligationFulfiller(
                updatePuller, memberStateMachine, serverId, dependencies ) ) );


        // Create HA services
        lockManager = dependencies.satisfyDependency(createLockManager( memberStateMachine, config, masterDelegateInvocationHandler, requestContextFactory, platformModule.availabilityGuard, logging ));

        propertyKeyTokenHolder = life.add( dependencies.satisfyDependency( new PropertyKeyTokenHolder(
                createPropertyKeyCreator( config, memberStateMachine, masterDelegateInvocationHandler, requestContextFactory, kernelProvider ) )));
        labelTokenHolder = life.add( dependencies.satisfyDependency(new LabelTokenHolder( createLabelIdCreator( config,
                memberStateMachine, masterDelegateInvocationHandler, requestContextFactory, kernelProvider ) ) ) );
        relationshipTypeTokenHolder = life.add( dependencies.satisfyDependency( new RelationshipTypeTokenHolder(
                createRelationshipTypeCreator( config, memberStateMachine, masterDelegateInvocationHandler,
                        requestContextFactory, kernelProvider ) ) ) );

        life.add( dependencies.satisfyDependency(createKernelData( config, platformModule.graphDatabaseFacade, members, lastUpdateTime ) ));

        commitProcessFactory = createCommitProcessFactory( dependencies, logging, monitors, config, life,
                null, members, platformModule.jobScheduler, master, requestContextFactory, memberStateMachine );

        headerInformationFactory = createHeaderInformationFactory( memberContext );

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

        upgradeConfiguration = new HAUpgradeConfiguration();

        registerRecovery( config.get( GraphDatabaseFacadeFactory.Configuration.editionName ), dependencies, logging );
    }

    private HazelcastInstance createHazelcastInstance( Config config )
    {
        JoinConfig joinConfig = new JoinConfig();
        joinConfig.getMulticastConfig().setEnabled( false );
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
        tcpIpConfig.setEnabled( true );

        List<HostnamePort> hostnamePorts = config.get( ClusterSettings.initial_hosts );
        for ( HostnamePort hostnamePort : hostnamePorts )
        {
            tcpIpConfig.addMember( hostnamePort.getHost() + ":" + hostnamePort.getPort() );
        }

        NetworkConfig networkConfig = new NetworkConfig();
        networkConfig.setPort( config.get( ClusterSettings.cluster_server ).getPort() );
        networkConfig.setJoin( joinConfig );
//        networkConfig.getInterfaces().setInterfaces( Arrays.asList( "127.0.0.1" ) ).setEnabled( true );
//        networkConfig.getInterfaces().setInterfaces( Arrays.asList( "192.168.1.12" ) ).setEnabled( true );
//        networkConfig.getInterfaces()
//                .setInterfaces( Arrays.asList(config.get( ClusterSettings.cluster_server ).getHost()) )
//                .setEnabled( true );

        com.hazelcast.config.Config c = new com.hazelcast.config.Config( String.valueOf( config.get( ClusterSettings.server_id ).toIntegerIndex() ) );
        c.setProperty( "hazelcast.initial.min.cluster.size", "2" );
        c.setNetworkConfig( networkConfig );

        return Hazelcast.newHazelcastInstance( c );
    }

    protected TransactionHeaderInformationFactory createHeaderInformationFactory( final HighAvailabilityMemberContext
                                                                                          memberContext )
    {
        return new TransactionHeaderInformationFactory.WithRandomBytes()
        {
            @Override
            protected TransactionHeaderInformation createUsing( byte[] additionalHeader )
            {
                return new TransactionHeaderInformation( memberContext.getElectedMasterId().toIntegerIndex(),
                        memberContext.getMyId().toIntegerIndex(), additionalHeader );
            }
        };
    }

    protected CommitProcessFactory createCommitProcessFactory( Dependencies dependencies, LogService logging,
                                                               Monitors monitors, Config config, LifeSupport life,
                                                               ClusterClient clusterClient, ClusterMembers members,
                                                               JobScheduler jobScheduler, final Master master,
                                                               final RequestContextFactory requestContextFactory,
                                                               final HighAvailabilityMemberStateMachine memberStateMachine )
    {
        final DelegateInvocationHandler<TransactionCommitProcess> commitProcessDelegate =
                new DelegateInvocationHandler<>( TransactionCommitProcess.class );

        DefaultSlaveFactory slaveFactory = dependencies.satisfyDependency( new DefaultSlaveFactory( logging.getInternalLogProvider(), monitors,
                config.get( HaSettings.com_chunk_size ).intValue() ) );

        Slaves slaves = dependencies.satisfyDependency(
                life.add( new HighAvailabilitySlaves( members, clusterClient, slaveFactory ) ) );

        final TransactionPropagator pusher = life.add( new TransactionPropagator( TransactionPropagator.from( config ),
                logging.getInternalLog( TransactionPropagator.class ), slaves, new CommitPusher( jobScheduler ) ) );

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
                if ( config.get( GraphDatabaseSettings.read_only ) )
                {
                    return new ReadOnlyTransactionCommitProcess();
                }
                else
                {

                    TransactionCommitProcess inner = new TransactionRepresentationCommitProcess( logicalTransactionStore, kernelHealth,
                                                neoStore, storeApplier, indexUpdatesValidator, mode );
                    new CommitProcessSwitcher( pusher, master, commitProcessDelegate, requestContextFactory,
                            memberStateMachine, txValidator, inner );

                    return (TransactionCommitProcess) Proxy
                            .newProxyInstance( TransactionCommitProcess.class.getClassLoader(),
                                    new Class[]{TransactionCommitProcess.class}, commitProcessDelegate );
                }
            }
        };
    }

    protected IdGeneratorFactory createIdGeneratorFactory(DelegateInvocationHandler<Master> masterDelegateInvocationHandler, LogProvider logging, RequestContextFactory requestContextFactory)
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

    protected Locks createLockManager(HighAvailabilityMemberStateMachine memberStateMachine, final Config config, DelegateInvocationHandler<Master> masterDelegateInvocationHandler,
                                      RequestContextFactory requestContextFactory, AvailabilityGuard availabilityGuard, final LogService logging)
    {
        DelegateInvocationHandler<Locks> lockManagerDelegate = new DelegateInvocationHandler<>( Locks.class );
        final Locks lockManager = (Locks) Proxy.newProxyInstance(
                Locks.class.getClassLoader(), new Class[]{Locks.class}, lockManagerDelegate );
        new LockManagerModeSwitcher( memberStateMachine, lockManagerDelegate, masterDelegateInvocationHandler,
                requestContextFactory, availabilityGuard, config, new Factory<Locks>()
        {
            @Override
            public Locks newInstance()
            {
                return CommunityEditionModule.createLockManager( config, logging );
            }
        } );
        return lockManager;
    }

    protected TokenCreator createRelationshipTypeCreator(Config config, HighAvailabilityMemberStateMachine memberStateMachine,
                                                         DelegateInvocationHandler<Master> masterDelegateInvocationHandler, RequestContextFactory requestContextFactory,
                                                         Supplier<KernelAPI> kernelProvider)
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

    protected TokenCreator createPropertyKeyCreator(Config config, HighAvailabilityMemberStateMachine memberStateMachine,
                                                             DelegateInvocationHandler<Master> masterDelegateInvocationHandler, RequestContextFactory requestContextFactory,
                                                             Supplier<KernelAPI> kernelProvider)
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

    protected TokenCreator createLabelIdCreator( Config config, HighAvailabilityMemberStateMachine memberStateMachine,
                                                             DelegateInvocationHandler<Master> masterDelegateInvocationHandler, RequestContextFactory requestContextFactory,
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

    protected KernelData createKernelData( Config config, GraphDatabaseAPI graphDb, ClusterMembers members, LastUpdateTime lastUpdateTime)
    {
        OnDiskLastTxIdGetter txIdGetter = new OnDiskLastTxIdGetter( graphDb );
        ClusterDatabaseInfoProvider databaseInfo = new ClusterDatabaseInfoProvider(
                members, txIdGetter, lastUpdateTime );
        return new HighlyAvailableKernelData( graphDb, members, databaseInfo, config );
    }

    protected void registerRecovery( final String editionName, final DependencyResolver dependencyResolver, final LogService logging)
    {
        memberStateMachine.addHighAvailabilityMemberListener( new HighAvailabilityMemberListener()
        {
            @Override
            public void masterIsElected( HighAvailabilityMemberChangeEvent event )
            {
            }

            @Override
            public void masterIsAvailable( HighAvailabilityMemberChangeEvent event )
            {
                if ( event.getOldState().equals( HighAvailabilityMemberState.TO_MASTER ) && event.getNewState().equals(
                        HighAvailabilityMemberState.MASTER ) )
                {
                    doAfterRecoveryAndStartup( true );
                }
            }

            @Override
            public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
            {
                if ( event.getOldState().equals( HighAvailabilityMemberState.TO_SLAVE ) && event.getNewState().equals(
                        HighAvailabilityMemberState.SLAVE ) )
                {
                    doAfterRecoveryAndStartup( false );
                }
            }

            @Override
            public void instanceStops( HighAvailabilityMemberChangeEvent event )
            {
            }

            private void doAfterRecoveryAndStartup( boolean isMaster )
            {
                try
                {
                    EnterpriseEditionModule.this.doAfterRecoveryAndStartup( editionName, dependencyResolver, isMaster );
                }
                catch ( Throwable throwable )
                {
                    Log messagesLog = logging.getInternalLog( EnterpriseEditionModule.class );
                    messagesLog.error( "Post recovery error", throwable );
                    try
                    {
                        memberStateMachine.stop();
                    }
                    catch ( Throwable throwable1 )
                    {
                        messagesLog.warn( "Could not stop", throwable1 );
                    }
                    try
                    {
                        memberStateMachine.start();
                    }
                    catch ( Throwable throwable1 )
                    {
                        messagesLog.warn( "Could not start", throwable1 );
                    }
                }
            }
        } );
    }

    protected void doAfterRecoveryAndStartup( String editionName, DependencyResolver resolver, boolean isMaster )
    {
        super.doAfterRecoveryAndStartup( editionName, resolver );

        if ( isMaster )
        {
            new RemoveOrphanConstraintIndexesOnStartup( resolver.resolveDependency( NeoStoreDataSource.class )
                    .getKernel(), resolver.resolveDependency( LogService.class ).getInternalLogProvider() ).perform();
        }
    }

    private static final class HAUpgradeConfiguration implements UpgradeConfiguration
    {
        @Override
        public void checkConfigurationAllowsAutomaticUpgrade()
        {
            throw new UpgradeNotAllowedByDatabaseModeException();
        }
    }
}
