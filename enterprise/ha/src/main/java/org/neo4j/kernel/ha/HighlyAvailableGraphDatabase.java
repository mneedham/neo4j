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
package org.neo4j.kernel.ha;

import java.util.Map;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.factory.EnterpriseEditionModule;
import org.neo4j.kernel.ha.factory.EnterpriseFacadeFactory;
import org.neo4j.kernel.impl.factory.DataSourceModule;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.GraphDatabaseDependencies.newDependencies;

/**
 * This has all the functionality of an embedded database, with the addition of services
 * for handling clustering.
 */
public class HighlyAvailableGraphDatabase extends GraphDatabaseFacade
{
    private EnterpriseEditionModule enterpriseEditionModule;

    public HighlyAvailableGraphDatabase( String storeDir, Map<String,String> params,
                                         Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                         Monitors monitors )
    {
        this( storeDir, params, newDependencies()
                .settingsClasses( GraphDatabaseSettings.class, ClusterSettings.class, HaSettings.class )
                .kernelExtensions( kernelExtensions ).monitors( monitors ) );
    }

    public HighlyAvailableGraphDatabase( String storeDir, Map<String,String> params,
                                         Iterable<KernelExtensionFactory<?>> kernelExtensions )
    {
        this( storeDir, params, newDependencies()
                .settingsClasses( GraphDatabaseSettings.class, ClusterSettings.class, HaSettings.class )
                .kernelExtensions( kernelExtensions ) );
    }

    public HighlyAvailableGraphDatabase( String storeDir, Map<String,String> params, GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        params.put( GraphDatabaseSettings.store_dir.name(), storeDir );
        new EnterpriseFacadeFactory().newFacade( params, dependencies, this );
    }

    @Override
    public void init( PlatformModule platformModule, EditionModule editionModule, DataSourceModule dataSourceModule )
    {
        super.init( platformModule, editionModule, dataSourceModule );
        this.enterpriseEditionModule = (EnterpriseEditionModule) editionModule;
    }

    public HighAvailabilityMemberState getInstanceState()
    {
        return enterpriseEditionModule.memberStateMachine.getCurrentState();
    }

    public String role()
    {
        return enterpriseEditionModule.members.getSelf().getHARole();
    }

    public boolean isMaster()
    {
        return enterpriseEditionModule.memberStateMachine.isMaster();
    }
}
