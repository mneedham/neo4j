package org.neo4j.coreedge.scenarios;


import java.io.File;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

@Command(name = "import", description = "Import classic neo4j database into core-edge format")
public class ImportClassicStoreCliCommand implements Runnable
{
    @Option(type = OptionType.COMMAND,
            name = {"--from"},
            description = "Path to existing classic neo4j database",
            title = "from",
            required = true)
    private File from;

    @Option(type = OptionType.COMMAND,
            name = {"--to"},
            description = "Path to destination core-edge format neo4j database",
            title = "directory",
            required = true)
    private File to;

    @Override
    public void run()
    {
        try
        {
            new ImportClassicStoreCommand( from, to ).execute();
        }
        catch ( Exception e )
        {
            e.printStackTrace( System.err );
            System.exit( -1 );
        }
    }
}
