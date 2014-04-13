package org.neo4j.desktop.config.osx;

import java.io.File;
import java.io.IOException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.plist.XMLPropertyListConfiguration;

public class PList
{
    private File pListFileLocation;

    private PList( File pListFileLocation )
    {
        this.pListFileLocation = pListFileLocation;
    }

    public static PList create( File pListFileLocation )
    {
        if ( !pListFileLocation.exists() )
        {
            try
            {
                pListFileLocation.createNewFile();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        return new PList( pListFileLocation );
    }

    public void save( String key, String value )
    {
        try
        {
            if ( pListFileLocation.length() == 0 )
            {
                XMLPropertyListConfiguration plist = new XMLPropertyListConfiguration();
                plist.setProperty( key, value );
                plist.save( pListFileLocation );
            }
            else
            {
                XMLPropertyListConfiguration plist = new XMLPropertyListConfiguration( pListFileLocation );
                plist.setProperty( key, value );
                plist.save();
            }
        }
        catch ( ConfigurationException e )
        {
            throw new RuntimeException( e );
        }

    }

    public String load( String key )
    {
        try
        {
            XMLPropertyListConfiguration plist = new XMLPropertyListConfiguration( pListFileLocation );
            return (String) plist.getProperty( key );
        }
        catch ( ConfigurationException e )
        {
            return null;
        }
    }
}
