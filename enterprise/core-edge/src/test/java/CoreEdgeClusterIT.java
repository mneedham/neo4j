import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.Cluster;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;

public class CoreEdgeClusterIT
{
    public final @Rule
    TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldBeAbleToFormACoreServerCluster()
    {
        // given
        Cluster cluster = new Cluster(dir.directory());

        // when
        cluster.start(3, 0);
        cluster.waitAllAvailable();

        // then
        assertEquals(3, cluster.getCoreServers());

        cluster.shutdown();
    }

    @Test
    public void shouldBeAbleToFormAMixedCoreAndEdgeCluster()
    {
        // given
        Cluster cluster = new Cluster(dir.directory());

        // when
        cluster.start(3, 3);
        cluster.waitAllAvailable();

        // then
        assertEquals(3, cluster.getCoreServers());
        assertEquals(3, cluster.getEdgeServers());

        cluster.shutdown();
    }
}
