package org.neo4j.kernel.ha.factory;

import java.io.File;
import java.util.Map;

import org.neo4j.coreedge.EnterpriseCoreEdgeEditionModule;
import org.neo4j.kernel.impl.factory.DataSourceModule;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;

/**
 * This facade creates instances of the Enterprise edition of Neo4j.
 */
public class EnterpriseCoreEdgeFacadeFactory extends GraphDatabaseFacadeFactory
{
    @Override
    public GraphDatabaseFacade newFacade( File storeDir, Map<String, String> params, Dependencies dependencies,
            GraphDatabaseFacade
            graphDatabaseFacade )
    {
        params.put( Configuration.editionName.name(), "Enterprise");
        return super.newFacade( storeDir, params, dependencies, graphDatabaseFacade );
    }

    @Override
    protected EditionModule createEdition( PlatformModule platformModule )
    {
        return new EnterpriseCoreEdgeEditionModule(platformModule);
    }

}
