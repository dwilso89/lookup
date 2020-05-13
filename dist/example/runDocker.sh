#!/usr/bin/env bash
# To use: execute script with one argument, the name of the yml configuration file to use in the "docker" directory (i.e. ./runDocker.sh simple)
docker run -v "$(pwd)/docker/$1.yml:/opt/lookup/conf/config.yml" -v "$(pwd)/data:/opt/lookup/data" -p 8888:8888  lookup:1.0-SNAPSHOT
