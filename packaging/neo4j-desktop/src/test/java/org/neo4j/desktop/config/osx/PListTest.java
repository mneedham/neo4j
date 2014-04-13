package org.neo4j.desktop.config.osx;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PListTest
{
    private File plistFile;

    @Before
    public void setup() {
        plistFile = new File( "neo4j.plist" );
    }

    @After
    public void cleanup() {
        plistFile.delete();
    }

    @Test
    public void shouldSavePropertyValue()
    {
        // given
        PList pList = PList.create(  plistFile );

        // when
        pList.save("property1", "value1");

        // then
        assertEquals("value1", pList.load("property1"));
    }

    @Test
    public void shouldReplacePropertyValue()
    {
        // given
        PList pList = PList.create(  plistFile );
        pList.save("property1", "value1");

        // when
        pList.save("property1", "value2");

        // then
        assertEquals("value2", pList.load("property1"));
    }

    @Test
    public void shouldSavePropertyValuesToDisk()
    {
        // given
        PList.create(  plistFile ).save( "property1", "value1" );

        // when
        PList plist = PList.create( plistFile );

        // then
        assertEquals( "value1", plist.load( "property1" ) );
    }

    @Test
    public void shouldReturnNullIfPropertyDoesNotExist()
    {
        // given
        PList plist = PList.create( plistFile );

        // when
        assertEquals( null, plist.load( "property1" ) );
    }
}
