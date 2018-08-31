#!/usr/bin/env bash
set -e

#-------------------------------------------------------------------------------
# Generate a release tarball - run this from the root of the git repository.
#-------------------------------------------------------------------------------
git submodule update --recursive --init
mkdir -p build
./genversion.py --template packaging/qclient.spec.in --out packaging/qclient.spec

#-------------------------------------------------------------------------------
# Extract version number, we need this for the archive name
#-------------------------------------------------------------------------------
VERSION_FULL=$(./genversion.py --template-string "@VERSION_FULL@")
printf "Version: ${VERSION_FULL}\n"
FILENAME="qclient-${VERSION_FULL}"

#-------------------------------------------------------------------------------
# Make the archive
#-------------------------------------------------------------------------------
TARGET_PATH=$(basename $PWD)
echo "TARGET PATH: $TARGET_PATH"

pushd $PWD/..
tar --exclude '*/.git' --exclude "${TARGET_PATH}/build" -pcvzf ${TARGET_PATH}/build/${FILENAME}.tar.gz ${TARGET_PATH} --transform "s!^${TARGET_PATH}!${FILENAME}!" --show-transformed-names
popd
