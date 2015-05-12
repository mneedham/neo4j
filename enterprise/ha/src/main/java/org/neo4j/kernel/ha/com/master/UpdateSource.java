package org.neo4j.kernel.ha.com.master;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;

public interface UpdateSource
{
    Response<Void> pullUpdates( RequestContext context );
}
