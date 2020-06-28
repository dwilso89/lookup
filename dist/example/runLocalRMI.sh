#!/usr/bin/env bash

# Ensure RMI registry is running
if [[ $1 == "RMI" ]]; then
    rmiregistry -J-Djava.rmi.server.useCodebaseOnly=false &
fi

java -Xmx12288m -cp "./local/:./data/:../target/dist-1.0-SNAPSHOT-Final/lib/*" \
     -Djava.rmi.server.codebase=file:///Users/p72521/IdeaProjects/lookupserver/dist/target/dist-1.0-SNAPSHOT-Final/lib/client-1.0-SNAPSHOT.jar \
     -Djava.rmi.server.hostname=localhost \
     -Djava.security.policy=server.policy \
        dewilson.projects.lookup.server.RMILookUpServer $1 $2