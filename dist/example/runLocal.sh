#!/usr/bin/env bash
# To use: execute script with one argument, the name of the yml configuration file to use in the "local" directory (i.e. ./runLocal.sh simple)
java -Xmx12288m -cp "./local/:./data/:../target/lookup-dist-1.0-SNAPSHOT-Final/bin/*:../target/lookup-dist-1.0-SNAPSHOT-Final/lib/*" dewilson.projects.lookup.server.RapidoidRestServer config=$1
