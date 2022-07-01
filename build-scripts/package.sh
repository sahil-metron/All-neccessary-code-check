#!/bin/bash

set -e   # Stop script on errors

# Set up build variables
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SRC_DIR=$SCRIPT_DIR/../
DIST_DIR=$SCRIPT_DIR/../dist
APP_VERSION=${GITHUB_REF##*/} # If this is triggered from a tag in GitHub actions, use that as the version
if [ -z "$APP_VERSION" ]
then
      APP_VERSION="localdev"
fi

echo "App Version:"$APP_VERSION
COMMIT_ID=`git rev-parse --short=6 HEAD`

# Clean the dist directory
rm -rf $DIST_DIR
mkdir -p $DIST_DIR

# Add files to the dist directory
PACKAGE_NAME=devo-wiz-$APP_VERSION-$COMMIT_ID.tar.gz
tar cvf $DIST_DIR/$PACKAGE_NAME $SRC_DIR/agent/modules/wiz_data_puller/* \
  $SRC_DIR/agent/modules/__init__.py \
  $SRC_DIR/config \
  $SRC_DIR/config_internal
