#!/bin/bash -eux

title="Neo4j"
backgroundPictureName="graph_background.png"
applicationName="Neo4j Community"
finalDMGName="Neo4j.dmg"

rm -rf target/dmg && mkdir -p target/dmg
tar -C target/dmg -xf target/install4j/neo4j-community_macos_2_1-SNAPSHOT.tgz
cp -r src/main/distribution/.background target/dmg
ln -s /Applications target/dmg

pushd target

hdiutil create -volname ${title} -size 200m -srcfolder dmg/ -ov -format UDRW pack.temp.dmg
device=$(hdiutil attach -readwrite -noverify -noautoopen "pack.temp.dmg" | egrep '^/dev/' | sed 1q | awk '{print $1}')

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

sleep 5

#hdiutil detach ${device}
hdiutil convert "pack.temp.dmg" -ov -format UDZO -imagekey zlib-level=9 -o "${finalDMGName}"
rm -f pack.temp.dmg
