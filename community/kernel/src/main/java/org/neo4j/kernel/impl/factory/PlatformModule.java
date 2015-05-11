/*
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
import org.neo4j.kernel.impl.util.Dependencies;
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
    private final PageCache pageCache;

    private final Monitors monitors;

    private final GraphDatabaseFacade graphDatabaseFacade;

    private final org.neo4j.kernel.impl.util.Dependencies dependencies;

    private final LogService logging;

    private final LifeSupport life;

    private final File storeDir;

    private final DiagnosticsManager diagnosticsManager;

    private final Tracers tracers;

    private final Config config;

    private final FileSystemAbstraction fileSystem;

    private final DataSourceManager dataSourceManager;

    private final KernelExtensions kernelExtensions;

    private final JobScheduler jobScheduler;

    private final AvailabilityGuard availabilityGuard;

    private final TransactionCounters transactionMonitor;

    public PlatformModule(Map<String, String> params, final GraphDatabaseFacadeFactory.Dependencies externalDependencies,
                                                  final GraphDatabaseFacade graphDatabaseFacade)
    {
        dependencies = new org.neo4j.kernel.impl.util.Dependencies( new Supplier<DependencyResolver>()
        {
            @Override
            public DependencyResolver get()
            {
                return getDataSourceManager().getDataSource().getDependencyResolver();
            }
        } );
        life = getDependencies().satisfyDependency( createLife() );
        this.graphDatabaseFacade = getDependencies().satisfyDependency( graphDatabaseFacade );

        // SPI - provided services
        config = getDependencies().satisfyDependency( new Config( params, getSettingsClasses(
                externalDependencies.settingsClasses(), externalDependencies.kernelExtensions() ) ) );

        storeDir = getConfig().get( GraphDatabaseFacadeFactory.Configuration.store_dir );

        kernelExtensions = getDependencies().satisfyDependency( new KernelExtensions(
                externalDependencies.kernelExtensions(),
                getDependencies(),
                UnsatisfiedDependencyStrategies.fail() ) );


        fileSystem = getLife().add( getDependencies().satisfyDependency( createFileSystemAbstraction() ) );

        // Component monitoring
        monitors = externalDependencies.monitors() == null ? new Monitors() : externalDependencies.monitors();
        getDependencies().satisfyDependency( getMonitors() );

        jobScheduler = getLife().add( getDependencies().satisfyDependency( createJobScheduler() ) );

        // If no logging was passed in from the outside then create logging and register
        // with this life
        logging = getLife().add( getDependencies()
                .satisfyDependency( createLogService( externalDependencies.userLogProvider() ) ) );

        getConfig().setLogger( getLogging().getInternalLog( Config.class ) );

        StoreLockerLifecycleAdapter storeLocker = getLife()
                .add( getDependencies().satisfyDependency( new StoreLockerLifecycleAdapter(
                        new StoreLocker( getFileSystem() ), getStoreDir() ) ) );

        new JvmChecker( getLogging().getInternalLog( JvmChecker.class ), new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();

        String desiredImplementationName = getConfig().get( GraphDatabaseFacadeFactory.Configuration.tracer );
        tracers = getDependencies().satisfyDependency(
                new Tracers( desiredImplementationName, getLogging().getInternalLog( Tracers.class ) ) );
        getDependencies().satisfyDependency( getTracers().pageCacheTracer );

        pageCache = getDependencies()
                .satisfyDependency( createPageCache( getFileSystem(), getConfig(), getLogging(), getTracers() ) );
        getLife().add( new PageCacheLifecycle( getPageCache() ) );

        diagnosticsManager = getLife()
                .add( getDependencies().satisfyDependency( new DiagnosticsManager( getLogging().getInternalLog(
                        DiagnosticsManager.class ) ) ) );

        // TODO please fix the bad dependencies instead of doing this. Before the removal of JTA
        // this was the place of the XaDataSourceManager. NeoStoreXaDataSource is create further down than
        // (specifically) KernelExtensions, which creates an interesting out-of-order issue with #doAfterRecovery().
        // Anyways please fix this.
        dataSourceManager = getLife().add( getDependencies().satisfyDependency( new DataSourceManager() ) );

        availabilityGuard = new AvailabilityGuard( Clock.SYSTEM_CLOCK );

        transactionMonitor = getDependencies().satisfyDependency( createTransactionCounters() );

    }

    public LifeSupport createLife()
    {
        return new LifeSupport();
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

        long internalLogRotationThreshold = getConfig()
                .get( GraphDatabaseSettings.store_internal_log_rotation_threshold );
        int internalLogRotationDelay = getConfig().get( GraphDatabaseSettings.store_internal_log_rotation_delay );
        int internalLogMaxArchives = getConfig().get( GraphDatabaseSettings.store_internal_log_max_archives );
        LogService logService;
        try
        {
            logService = new StoreLogService( userLogProvider, getFileSystem(), getStoreDir(),
                    internalLogRotationThreshold, internalLogRotationDelay, internalLogMaxArchives,
                    getJobScheduler(), new Consumer<LogProvider>()
            {
                @Override
                public void accept( LogProvider logProvider )
                {
                    getDiagnosticsManager().dumpAll( logProvider.getLog( DiagnosticsManager.class ) );
                }
            } );
        } catch ( IOException ex )
        {
            throw new RuntimeException( ex );
        }
        return getLife().add( logService );
    }

    protected Neo4jJobScheduler createJobScheduler()
    {
        return new Neo4jJobScheduler( getConfig().get( GraphDatabaseFacadeFactory.Configuration.editionName ));
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

    public PageCache getPageCache()
    {
        return pageCache;
    }

    public Monitors getMonitors()
    {
        return monitors;
    }

    public GraphDatabaseFacade getGraphDatabaseFacade()
    {
        return graphDatabaseFacade;
    }

    public Dependencies getDependencies()
    {
        return dependencies;
    }

    public LogService getLogging()
    {
        return logging;
    }

    public LifeSupport getLife()
    {
        return life;
    }

    public DiagnosticsManager getDiagnosticsManager()
    {
        return diagnosticsManager;
    }

    public Tracers getTracers()
    {
        return tracers;
    }

    public Config getConfig()
    {
        return config;
    }

    public FileSystemAbstraction getFileSystem()
    {
        return fileSystem;
    }

    public DataSourceManager getDataSourceManager()
    {
        return dataSourceManager;
    }

    public KernelExtensions getKernelExtensions()
    {
        return kernelExtensions;
    }

    public JobScheduler getJobScheduler()
    {
        return jobScheduler;
    }

    public AvailabilityGuard getAvailabilityGuard()
    {
        return availabilityGuard;
    }

    public TransactionCounters getTransactionMonitor()
    {
        return transactionMonitor;
    }

    public File getStoreDir()
    {
        return storeDir;
    }
}
