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
package org.neo4j.server.enterprise;

import java.io.File;
import java.util.Map;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.coreedge.HazelcastClusterManagement;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.coreedge.CoreGraphDatabase;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.InterruptThreadTimer;
import org.neo4j.server.advanced.AdvancedNeoServer;
import org.neo4j.server.configuration.ConfigurationBuilder;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.LifecycleManagingDatabase.GraphFactory;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.preflight.EnsurePreparedForHttpLogging;
import org.neo4j.server.preflight.PerformRecoveryIfNecessary;
import org.neo4j.server.preflight.PerformUpgradeIfNecessary;
import org.neo4j.server.preflight.PreFlightTasks;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.web.ServerInternalSettings;
import org.neo4j.server.webadmin.rest.MasterInfoServerModule;
import org.neo4j.server.webadmin.rest.MasterInfoService;

import static java.util.Arrays.asList;

import static org.neo4j.helpers.collection.Iterables.mix;
import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;

public class EnterpriseNeoServer extends AdvancedNeoServer
{
    public static final String SINGLE = "SINGLE";
    public static final String HA = "HA";
    public static final String CORE_EDGE = "CORE_EDGE";

    private static final GraphFactory ENTERPRISE_FACTORY = new GraphFactory()
    {
        @Override
        public GraphDatabaseAPI newGraphDatabase( Config config, Dependencies dependencies )
        {

            File storeDir = config.get( ServerInternalSettings.legacy_db_location );
            return new HighlyAvailableGraphDatabase( storeDir, config.getParams(), dependencies );
        }
    };

    private static final GraphFactory CORE_EDGE_FACTORY = new GraphFactory()
    {
        @Override
        public GraphDatabaseAPI newGraphDatabase( Config config, Dependencies dependencies )
        {

            File storeDir = config.get( ServerInternalSettings.legacy_db_location );
            return new CoreGraphDatabase(
                    new HazelcastClusterManagement( config ),
                    storeDir,
                    config.getParams(),
                    dependencies );
        }
    };

    public EnterpriseNeoServer( ConfigurationBuilder configurator, Dependencies dependencies, LogProvider logProvider )
    {
        super( configurator, createDbFactory( configurator.configuration() ), dependencies, logProvider );
    }

    protected static Database.Factory createDbFactory( Config config )
    {
        final String mode = config.get( ServerInternalSettings.legacy_db_mode ).toUpperCase();

        switch ( mode )
        {
        case HA:
            return lifecycleManagingDatabase( ENTERPRISE_FACTORY );
        case CORE_EDGE:
            return lifecycleManagingDatabase( CORE_EDGE_FACTORY );
        default:
            return lifecycleManagingDatabase( COMMUNITY_FACTORY );
        }
    }

    @Override
    protected PreFlightTasks createPreflightTasks()
    {
        return new PreFlightTasks( logProvider,
                new EnsurePreparedForHttpLogging( configurator.configuration() ), new PerformUpgradeIfNecessary(
                getConfig(), configurator.getDatabaseTuningProperties(), logProvider,
                StoreUpgrader.NO_MONITOR ), new PerformRecoveryIfNecessary( getConfig(),
                configurator.getDatabaseTuningProperties(), logProvider ) );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected Iterable<ServerModule> createServerModules()
    {
        return mix(
                asList( (ServerModule) new MasterInfoServerModule( webServer, getConfig(),
                        logProvider ) ), super.createServerModules() );
    }

    @Override
    protected InterruptThreadTimer createInterruptStartupTimer()
    {
        // If we are in HA mode, database startup can take a very long time, so
        // we default to disabling the startup timeout here, unless explicitly overridden
        // by configuration.
        if ( getConfig().get( ServerInternalSettings.legacy_db_mode ).equalsIgnoreCase( "ha" ) )
        {
            final long startupTimeout = getStartTimeoutFromPropertiesOrSetToZeroIfNoKeyFound();
            InterruptThreadTimer stopStartupTimer;
            if ( startupTimeout > 0 )
            {
                stopStartupTimer = InterruptThreadTimer.createTimer( startupTimeout, Thread.currentThread() );
            }
            else
            {
                stopStartupTimer = InterruptThreadTimer.createNoOpTimer();
            }
            return stopStartupTimer;
        }
        else
        {
            return super.createInterruptStartupTimer();
        }
    }

    private long getStartTimeoutFromPropertiesOrSetToZeroIfNoKeyFound()
    {
        long startupTimeout;
        final Map<String,String> params = getConfig().getParams();
        if ( params.containsKey( ServerInternalSettings.startup_timeout.name() ) )
        {
            startupTimeout = getConfig().get( ServerInternalSettings.startup_timeout );
        }
        else
        {
            startupTimeout = 0;
        }
        return startupTimeout;
    }

    @Override
    public Iterable<AdvertisableService> getServices()
    {
        if ( getDatabase().getGraph() instanceof HighlyAvailableGraphDatabase )
        {
            return Iterables.append( new MasterInfoService( null, null ), super.getServices() );
        }
        else
        {
            return super.getServices();
        }
    }
}
