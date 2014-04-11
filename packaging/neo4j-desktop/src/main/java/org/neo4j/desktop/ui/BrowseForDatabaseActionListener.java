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
package org.neo4j.desktop.ui;

import java.awt.FileDialog;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JTextField;

import static javax.swing.JFileChooser.APPROVE_OPTION;
import static javax.swing.JFileChooser.CUSTOM_DIALOG;
import static javax.swing.JFileChooser.DIRECTORIES_ONLY;
import static javax.swing.JOptionPane.CANCEL_OPTION;
import static javax.swing.JOptionPane.ERROR_MESSAGE;
import static javax.swing.JOptionPane.OK_CANCEL_OPTION;

import static org.neo4j.desktop.ui.ScrollableOptionPane.showWrappedConfirmDialog;

class BrowseForDatabaseActionListener implements ActionListener
{
    private final JFrame frame;
    private final JTextField directoryDisplay;
    private final DesktopModel model;
    private OpenFileDialog openFileDialog;

    public BrowseForDatabaseActionListener( JFrame frame, JTextField directoryDisplay, DesktopModel model,
                                            OpenFileDialog openFileDialog )
    {
        this.frame = frame;
        this.directoryDisplay = directoryDisplay;
        this.model = model;
        this.openFileDialog = openFileDialog;
    }

    @Override
    public void actionPerformed( ActionEvent e )
    {
        openFileDialog.open(e, frame, directoryDisplay, model);
    }
}
