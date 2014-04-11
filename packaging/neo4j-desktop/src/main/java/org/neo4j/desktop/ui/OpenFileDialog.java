package org.neo4j.desktop.ui;

import java.awt.event.ActionEvent;
import javax.swing.JFrame;
import javax.swing.JTextField;

public interface OpenFileDialog
{
    void open( ActionEvent e, JFrame frame, JTextField directoryDisplay, DesktopModel model );
}

