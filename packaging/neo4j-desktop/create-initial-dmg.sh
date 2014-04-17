#!/bin/bash -eux

title="Neo4j"
backgroundPictureName="graph_background.png"
applicationName="Neo4j Community"
templateDMGName="Neo4j.template.dmg"
templateZipName="Neo4j.template.zip"

if [ -f target/${templateDMGName} ] 
  then
  rm target/${templateDMGName}
fi

if [ -d target/dmg-template ] 
  then
  rm -rf target/dmg-template && mkdir -p target/dmg-template
fi

cp -R src/main/distribution/dmg-template target/

hdiutil create -volname ${title} -size 200m -srcfolder target/dmg-template/ -ov -format UDRW target/${templateDMGName}
device=$(hdiutil attach -readwrite -noverify -noautoopen "target/${templateDMGName}" | egrep '^/dev/' | sed 1q | awk '{print $1}')

sleep 5

echo '
   tell application "Finder"
     tell disk "'${title}'"
           open
           set current view of container window to icon view
           set toolbar visible of container window to false
           set statusbar visible of container window to false
           set the bounds of container window to {400, 100, 879, 446}
           set theViewOptions to the icon view options of container window
           set arrangement of theViewOptions to not arranged
           set icon size of theViewOptions to 72
           set background picture of theViewOptions to file ".background:'${backgroundPictureName}'"
           set position of item "'${applicationName}'" of container window to {100, 170}
           set position of item "Applications" of container window to {375, 170}
           update without registering applications
           delay 5
           eject
     end tell
   end tell
' | osascript

zip target/${templateZipName} target/${templateDMGName}