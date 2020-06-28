#!/usr/bin/env bash
java -Xmx12288m -cp "./local/:./data/:../target/dist-1.0-SNAPSHOT-Final/lib/*" dewilson.projects.lookup.server.UnixDomainSocketHTTPServer
