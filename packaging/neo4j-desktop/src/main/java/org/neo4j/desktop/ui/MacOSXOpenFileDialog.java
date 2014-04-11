package org.neo4j.desktop.ui;

import java.awt.FileDialog;
import java.awt.event.ActionEvent;
import java.io.File;

import javax.swing.JFrame;
import javax.swing.JTextField;

import static javax.swing.JOptionPane.ERROR_MESSAGE;
import static javax.swing.JOptionPane.OK_CANCEL_OPTION;

import static org.neo4j.desktop.ui.ScrollableOptionPane.showWrappedConfirmDialog;

public class MacOSXOpenFileDialog implements OpenFileDialog {

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
