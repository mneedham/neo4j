#!/bin/bash -eux

finalDMGName="Neo4j.dmg"
templateDMGName="Neo4j.template.dmg"
tempDMGName="Neo4j.temp.dmg"

cp target/${templateDMGName} target/${tempDMGName}

device=$(hdiutil attach -readwrite -noverify -noautoopen "target/${tempDMGName}" | egrep '^/dev/' | sed 1q | awk '{print $1}')

tar -C /Volumes/Neo4j -xf target/install4j/neo4j-community_macos_2_1-SNAPSHOT.tgz

hdiutil detach ${device}

hdiutil convert "target/${tempDMGName}" -ov -format UDZO -imagekey zlib-level=9 -o "target/${finalDMGName}"
rm -f target/Neo4j.temp.dmg
