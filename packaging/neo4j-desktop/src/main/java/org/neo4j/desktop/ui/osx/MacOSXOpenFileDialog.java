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
package org.neo4j.desktop.ui.osx;

import java.awt.FileDialog;
import java.awt.event.ActionEvent;
import java.io.File;

import javax.swing.JFrame;
import javax.swing.JTextField;

import org.neo4j.desktop.ui.DesktopModel;
import org.neo4j.desktop.ui.OpenFileDialog;
import org.neo4j.desktop.ui.UnsuitableDirectoryException;

import static javax.swing.JOptionPane.ERROR_MESSAGE;
import static javax.swing.JOptionPane.OK_CANCEL_OPTION;

import static org.neo4j.desktop.ui.ScrollableOptionPane.showWrappedConfirmDialog;

public class MacOSXOpenFileDialog implements OpenFileDialog
{

    @Override
    public void open( ActionEvent e, JFrame frame, JTextField directoryDisplay, DesktopModel model )
    {
        System.setProperty( "apple.awt.fileDialogForDirectories", "true" );

        FileDialog fileDialog = new FileDialog( frame );
        fileDialog.setDirectory( directoryDisplay.getText() );
        fileDialog.setTitle( "Select database" );

        fileDialog.setVisible( true );
        System.setProperty( "apple.awt.fileDialogForDirectories", "false" );

        String selectedFile = fileDialog.getFile();

        if(selectedFile == null) {
            return;
        }

        try
        {
            model.setDatabaseDirectory( new File(fileDialog.getDirectory(), selectedFile ));
            directoryDisplay.setText( model.getDatabaseDirectory().getAbsolutePath() );
        }
        catch ( UnsuitableDirectoryException error )
        {
            showWrappedConfirmDialog(
                    frame, error.getMessage() + "\nPlease choose a different folder.",
                    "Invalid folder selected", OK_CANCEL_OPTION, ERROR_MESSAGE );

        }
    }
}
