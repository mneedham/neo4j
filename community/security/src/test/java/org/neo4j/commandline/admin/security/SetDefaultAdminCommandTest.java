/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.commandline.admin.security;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;

import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.Credential;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.User;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.neo4j.test.assertion.Assert.assertException;

public class SetDefaultAdminCommandTest
{
    private SetDefaultAdminCommand setDefaultAdmin;
    private File adminIniFile;
    private FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
    private Config config;

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory( fileSystem );
    private UserRepository users;

    @Before
    public void setup() throws IOException, InvalidArgumentsException
    {
        OutsideWorld mock = Mockito.mock( OutsideWorld.class );
        when( mock.fileSystem() ).thenReturn( fileSystem );
        setDefaultAdmin = new SetDefaultAdminCommand( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath(), mock );
        config = setDefaultAdmin.loadNeo4jConfig();
        users = CommunitySecurityModule.getUserRepository( config, NullLogProvider.getInstance(), fileSystem );
        users.create(
                new User.Builder( "jake", Credential.forPassword( "123" ) )
                        .withRequiredPasswordChange( false )
                        .build()
            );
        adminIniFile = new File( CommunitySecurityModule.getUserRepositoryFile( config ).getParentFile(), "admin.ini" );
    }

    @Test
    public void shouldFailForNoArguments() throws Exception
    {
        assertException( () -> setDefaultAdmin.execute( new String[0] ), IncorrectUsage.class,
                "No username specified." );
    }

    @Test
    public void shouldFailForTooManyArguments() throws Exception
    {
        String[] arguments = {"", "123", "321"};
        assertException( () -> setDefaultAdmin.execute( arguments ), IncorrectUsage.class, "Too many arguments." );
    }

    @Test
    public void shouldSetDefaultAdmin() throws Throwable
    {
        // Given
        assertFalse( fileSystem.fileExists( adminIniFile ) );

        // When
        String[] arguments = {"jake"};
        setDefaultAdmin.execute( arguments );

        // Then
        assertAdminIniFile( "jake" );
    }

    @Test
    public void shouldSetDefaultAdminEvenForNonExistentUser() throws Throwable
    {
        // Given
        assertFalse( fileSystem.fileExists( adminIniFile ) );

        // When
        String[] arguments = {"noName"};
        setDefaultAdmin.execute( arguments );

        // Then
        assertAdminIniFile( "noName" );
    }

    private void assertAdminIniFile( String username ) throws Throwable
    {
        assertTrue( fileSystem.fileExists( adminIniFile ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, adminIniFile,
            NullLogProvider.getInstance() );
        userRepository.start();
        assertThat( userRepository.getAllUsernames(), containsInAnyOrder( username ) );
    }
}
