/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
