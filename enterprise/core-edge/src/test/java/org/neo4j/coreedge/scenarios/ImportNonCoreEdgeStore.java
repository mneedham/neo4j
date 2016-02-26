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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Collection;

import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.help.Help;

import static java.util.Arrays.asList;

@Command( name = "import", description = "Import a classic neo4j store into the core-edge format." )
public class ImportNonCoreEdgeStore
{

    private static final Cli<Runnable> PARSER = Cli.<Runnable>builder( "neo-integration" )
            .withDescription( "Neo4j integration tools." )
            .withDefaultCommand( Help.class )
            .withCommand( Help.class )
            .build();

    public static void main( String[] args )
    {
        CliRunner.run( PARSER, args );
    }

    static Collection<String> executeMainReturnSysOut( String[] args )
    {
        PrintStream oldOut = System.out;

        ByteArrayOutputStream newOut = new ByteArrayOutputStream();
        System.setOut( new PrintStream( newOut ) );

        CliRunner.run( PARSER, args, CliRunner.OnCommandFinished.DoNothing );

        try
        {
            return asList( newOut.toString( "UTF8" ).split( System.lineSeparator() ) );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            System.setOut( oldOut );
        }
    }
}
