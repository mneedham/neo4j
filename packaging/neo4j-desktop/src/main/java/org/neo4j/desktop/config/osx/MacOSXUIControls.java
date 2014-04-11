package org.neo4j.desktop.config.osx;

import org.neo4j.desktop.ui.MacOSXOpenFileDialog;
import org.neo4j.desktop.ui.OpenFileDialog;

public class MacOSXUIControls implements UIControls
{
    @Override
    public OpenFileDialog getOpenFileDialog()
    {
        return new MacOSXOpenFileDialog();
    }
}
