/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.desktop.config.osx;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.plist.XMLPropertyListConfiguration;

import org.neo4j.desktop.ui.UIControls;
import org.neo4j.desktop.config.unix.UnixInstallation;
import org.neo4j.desktop.ui.osx.MacOSXUIControls;

public class DarwinInstallation extends UnixInstallation
{

    private final String userHomeDirectory;

    public DarwinInstallation()
    {
        userHomeDirectory = System.getProperty( "user.home" );
    }

    @Override
    protected File getDefaultDirectory()
    {
        // cf. http://stackoverflow.com/questions/567874/how-do-i-find-the-users-documents-folder-with-java-in-os-x
        return new File( new File( userHomeDirectory ), "Documents" );
    }

    @Override
    public void setDatabaseDirectory( File location )
    {
        PList plist = PList.create( new File(getPListFileLocation()) );
        plist.save( "databaseDirectory", location.getAbsolutePath() );
    }

    private String getPListFileLocation()
    {
        return String.format( "%s/Library/Preferences/org.neo4j.desktop.config.plist", userHomeDirectory );
    }

    @Override
    public File getDatabaseDirectory()
    {
        String pListFileLocation = getPListFileLocation();

        if ( !new File( pListFileLocation ).exists() )
        {
            File defaultDatabaseDirectory = getDefaultDatabaseDirectory();
            setDatabaseDirectory( defaultDatabaseDirectory );
            return defaultDatabaseDirectory;
        }
        else
        {
            PList plist = PList.create( new File(pListFileLocation) );
            return new File(plist.load( "databaseDirectory" ));
        }
    }

    @Override
    public UIControls getUIControls()
    {
        return new MacOSXUIControls();
    }
}
