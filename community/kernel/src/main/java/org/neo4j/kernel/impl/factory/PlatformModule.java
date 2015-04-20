/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.factory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.neo4j.function.Consumer;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.StoreLocker;
import org.neo4j.kernel.StoreLockerLifecycleAdapter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.extension.UnsatisfiedDependencyStrategies;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.pagecache.PageCacheLifecycle;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.info.JvmChecker;
import org.neo4j.kernel.info.JvmMetadataRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

/**
 * Platform module for {@link org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory}. This creates
 * all the services needed by {@link org.neo4j.kernel.impl.factory.EditionModule} implementations.
 */
public class PlatformModule
{
    public PageCache pageCache;

    public Monitors monitors;

    public GraphDatabaseFacade graphDatabaseFacade;

    public org.neo4j.kernel.impl.util.Dependencies dependencies;

    public LogService logging;

    public LifeSupport life;

    public File storeDir;

    public DiagnosticsManager diagnosticsManager;

    public Tracers tracers;

    public Config config;

    public FileSystemAbstraction fileSystem;

    public DataSourceManager dataSourceManager;

    public KernelExtensions kernelExtensions;

    public JobScheduler jobScheduler;

    public AvailabilityGuard availabilityGuard;

    public TransactionCounters transactionMonitor;

    public PlatformModule(Map<String, String> params, final GraphDatabaseFacadeFactory.Dependencies externalDependencies,
                                                  final GraphDatabaseFacade graphDatabaseFacade)
    {
        dependencies = new org.neo4j.kernel.impl.util.Dependencies( new Supplier<DependencyResolver>()
        {
            @Override
            public DependencyResolver get()
            {
                return dataSourceManager.getDataSource().getDependencyResolver();
            }
        } );
        life = dependencies.satisfyDependency(new LifeSupport());
        this.graphDatabaseFacade = dependencies.satisfyDependency(graphDatabaseFacade);

        // SPI - provided services
        config = dependencies.satisfyDependency( new Config( params, getSettingsClasses(
                externalDependencies.settingsClasses(), externalDependencies.kernelExtensions() ) ) );

        storeDir = config.get( GraphDatabaseFacadeFactory.Configuration.store_dir );

        kernelExtensions = dependencies.satisfyDependency( new KernelExtensions(
                externalDependencies.kernelExtensions(),
                dependencies,
                UnsatisfiedDependencyStrategies.fail() ) );


        fileSystem = life.add( dependencies.satisfyDependency( createFileSystemAbstraction() ) );

        // Component monitoring
        monitors = externalDependencies.monitors() == null ? new Monitors() : externalDependencies.monitors();
        dependencies.satisfyDependency( monitors );

        jobScheduler = life.add( dependencies.satisfyDependency(createJobScheduler() ));

        // If no logging was passed in from the outside then create logging and register
        // with this life
        logging = life.add(dependencies.satisfyDependency(createLogService(externalDependencies.userLogProvider())));

        config.setLogger( logging.getInternalLog( Config.class ) );

        StoreLockerLifecycleAdapter storeLocker = life.add( dependencies.satisfyDependency( new StoreLockerLifecycleAdapter(
                new StoreLocker( fileSystem ), storeDir ) ));

        new JvmChecker( logging.getInternalLog( JvmChecker.class ), new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();

        String desiredImplementationName = config.get( GraphDatabaseFacadeFactory.Configuration.tracer );
        tracers = dependencies.satisfyDependency( new Tracers( desiredImplementationName, logging.getInternalLog( Tracers.class ) ) );
        dependencies.satisfyDependency( tracers.pageCacheTracer );

        pageCache = dependencies.satisfyDependency( createPageCache( fileSystem, config, logging, tracers ) );
        life.add( new PageCacheLifecycle( pageCache ) );

        diagnosticsManager = life.add( dependencies.satisfyDependency( new DiagnosticsManager( logging.getInternalLog(
                DiagnosticsManager.class ) ) ) );

        // TODO please fix the bad dependencies instead of doing this. Before the removal of JTA
        // this was the place of the XaDataSourceManager. NeoStoreXaDataSource is create further down than
        // (specifically) KernelExtensions, which creates an interesting out-of-order issue with #doAfterRecovery().
        // Anyways please fix this.
        dataSourceManager = life.add( dependencies.satisfyDependency(new DataSourceManager() ));

        availabilityGuard = new AvailabilityGuard( Clock.SYSTEM_CLOCK );

        transactionMonitor = dependencies.satisfyDependency( createTransactionCounters() );

    }

    protected FileSystemAbstraction createFileSystemAbstraction()
    {
        return new DefaultFileSystemAbstraction();
    }

    protected LogService createLogService(LogProvider userLogProvider)
    {
        if ( userLogProvider == null )
        {
            userLogProvider = NullLogProvider.getInstance();
        }

        long internalLogRotationThreshold = config.get( GraphDatabaseSettings.store_internal_log_rotation_threshold );
        int internalLogRotationDelay = config.get( GraphDatabaseSettings.store_internal_log_rotation_delay );
        int internalLogMaxArchives = config.get( GraphDatabaseSettings.store_internal_log_archive_count );
        LogService logService;
        try
        {
            logService = new StoreLogService( userLogProvider, fileSystem, storeDir,
                    internalLogRotationThreshold, internalLogRotationDelay, internalLogMaxArchives,
                    jobScheduler, new Consumer<LogProvider>()
            {
                @Override
                public void accept( LogProvider logProvider )
                {
                    diagnosticsManager.dumpAll( logProvider.getLog( DiagnosticsManager.class ) );
                }
            } );
        } catch ( IOException ex )
        {
            throw new RuntimeException( ex );
        }
        return life.add( logService );
    }

    protected Neo4jJobScheduler createJobScheduler()
    {
        return new Neo4jJobScheduler( config.get( GraphDatabaseFacadeFactory.Configuration.editionName ));
    }

    protected PageCache createPageCache( FileSystemAbstraction fileSystem, Config config, LogService logging, Tracers tracers)
    {
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fileSystem, config, tracers.pageCacheTracer );
        PageCache pageCache = pageCacheFactory.getOrCreatePageCache();

        if ( config.get( GraphDatabaseSettings.dump_configuration ) )
        {
            pageCacheFactory.dumpConfiguration( logging.getInternalLog( PageCache.class ) );
        }
        return pageCache;
    }

    protected TransactionCounters createTransactionCounters()
    {
        return new TransactionCounters();
    }

    private Iterable<Class<?>> getSettingsClasses( Iterable<Class<?>> settingsClasses,
                                                   Iterable<KernelExtensionFactory<?>> kernelExtensions)
    {
        List<Class<?>> totalSettingsClasses = Iterables.toList( settingsClasses );

        // Get the list of settings classes for extensions
        for ( KernelExtensionFactory<?> kernelExtension : kernelExtensions )
        {
            if ( kernelExtension.getSettingsClass() != null )
            {
                totalSettingsClasses.add( kernelExtension.getSettingsClass() );
            }
        }

        return totalSettingsClasses;
    }
}
