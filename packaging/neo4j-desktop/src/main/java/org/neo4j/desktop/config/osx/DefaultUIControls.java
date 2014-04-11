package org.neo4j.desktop.config.osx;

import org.neo4j.desktop.ui.DefaultOpenFileDialog;
import org.neo4j.desktop.ui.OpenFileDialog;

public class DefaultUIControls implements  UIControls {

    @Override
    public OpenFileDialog getOpenFileDialog()
    {
        return new DefaultOpenFileDialog();
    }
}
