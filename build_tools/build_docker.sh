#!/bin/bash

version=$(grep '"version"' metadata.json | awk -F ':' '{print $2}' |  tr -d '", ')
collector_name=$(grep '"package_name"' metadata.json | awk -F ':' '{print $2}' |  tr -d '", ')

docker build --compress --force-rm --no-cache --tag devo.com/collectors/${collector_name}:${version} .