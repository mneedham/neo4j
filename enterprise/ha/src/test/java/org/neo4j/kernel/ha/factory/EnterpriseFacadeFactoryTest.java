package org.neo4j.kernel.ha.factory;

import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.helpers.Clock;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLog;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class EnterpriseFacadeFactoryTest
{
    @Test
    public void shouldReturnEnterpriseModule()
    {
        // given
        EnterpriseFacadeFactory factory = new EnterpriseFacadeFactory();

        PlatformModule platformModule = createPlatformModule();

        createKernelExtensions( platformModule );

        Config config = new Config( params( ClusterSettings.ConsistencyMode.EVENTUAL ) );
        when( platformModule.getConfig() ).thenReturn( config );

        // when

        EditionModule module = factory.createEdition( platformModule );

        // then
        assertTrue( module instanceof EnterpriseEditionModule );
    }

    @Test
    public void shouldReturnEnterpriseCoreEdgeEditionModule()
    {
        // given
        EnterpriseFacadeFactory factory = new EnterpriseFacadeFactory();

        PlatformModule platformModule = createPlatformModule();

        createKernelExtensions( platformModule );

        Config config = new Config( params( ClusterSettings.ConsistencyMode.CORE_EDGE ) );
        when( platformModule.getConfig() ).thenReturn( config );

        // when
        EditionModule module = factory.createEdition( platformModule );

        // then
        assertTrue( module instanceof EnterpriseCoreEdgeEditionModule );
    }

    private Map<String,String> params( ClusterSettings.ConsistencyMode consistencyMode )
    {
        Map<String,String> params = new HashMap<>();
        params.put( ClusterSettings.server_id.name(), "1" );
        params.put( ClusterSettings.consistency_mode.name(),
                consistencyMode.name().toLowerCase() );
        return params;
    }

    private void createKernelExtensions( PlatformModule platformModule )
    {
        KernelExtensions kernelExtensions = mock( KernelExtensions.class );
        when( platformModule.getKernelExtensions() ).thenReturn( kernelExtensions );
        when( kernelExtensions.listFactories() )
                .thenReturn( Collections.<KernelExtensionFactory<?>>emptyList() );
    }

    private PlatformModule createPlatformModule()
    {
        PlatformModule platformModule = mock( PlatformModule.class );
        when( platformModule.getLogging() ).thenReturn( spy( NullLogService.getInstance() ) );
        when( platformModule.getStoreDir() ).thenReturn( mock( File.class ) );
        when( platformModule.getLife() ).thenReturn( new LifeSupport() );
        when( platformModule.getDependencies() ).thenReturn( new Dependencies() );
        when( platformModule.getMonitors() ).thenReturn( new Monitors() );
        when( platformModule.getAvailabilityGuard() ).thenReturn( new AvailabilityGuard( Clock.SYSTEM_CLOCK ) );
        when( platformModule.getDiagnosticsManager() ).thenReturn( new DiagnosticsManager( NullLog.getInstance() ) );
        return platformModule;
    }
}
