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

import java.io.File;
import java.util.Map;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.factory.EnterpriseCoreEdgeFacadeFactory;
import org.neo4j.kernel.ha.factory.EnterpriseFacadeFactory;
import org.neo4j.kernel.impl.factory.DataSourceModule;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.GraphDatabaseDependencies.newDependencies;

public class CoreGraphDatabase extends GraphDatabaseFacade
{
    private EnterpriseCoreEdgeEditionModule enterpriseCoreEdgeEditionModule;

    public CoreGraphDatabase( ClusterManagement management, File storeDir, Map<String,String> params,
            GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        new EnterpriseCoreEdgeFacadeFactory().newFacade( storeDir, params, dependencies, this );
    }

    @Override
    public void init( PlatformModule platformModule, EditionModule editionModule, DataSourceModule dataSourceModule )
    {
        super.init( platformModule, editionModule, dataSourceModule );
        this.enterpriseCoreEdgeEditionModule = (EnterpriseCoreEdgeEditionModule) editionModule;
    }
}
