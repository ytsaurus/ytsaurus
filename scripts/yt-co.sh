#!/bin/bash
# This script performs _initial_ sparse checkout for YT subtree.
# Please, keep it up to date.
set -x
set -e

# Do not include trailing slashes.
REPOSITORY=svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia
SOURCE=/Users/sandello/YT/source
BUILD=/Users/sandello/YT/build

mkdir -p $SOURCE
mkdir -p $BUILD

# Checkout repository skeleton.
svn checkout --depth immediates $REPOSITORY/ $SOURCE/

# Checkout required directories.
svn update --set-depth infinity $SOURCE/check
svn update --set-depth infinity $SOURCE/cmake
svn update --set-depth infinity $SOURCE/contrib
svn update --set-depth infinity $SOURCE/generated
svn update --set-depth infinity $SOURCE/kernel
svn update --set-depth infinity $SOURCE/yandex
svn update --set-depth infinity $SOURCE/library
svn update --set-depth infinity $SOURCE/unittest
svn update --set-depth infinity $SOURCE/util

svn update --set-depth empty $SOURCE/dict
svn update --set-depth infinity $SOURCE/dict/dictutil
svn update --set-depth infinity $SOURCE/dict/json

svn update --set-depth empty $SOURCE/quality
svn update --set-depth infinity $SOURCE/quality/mapreducelib
svn update --set-depth infinity $SOURCE/quality/random
svn update --set-depth infinity $SOURCE/quality/util
svn update --set-depth infinity $SOURCE/quality/Misc
svn update --set-depth infinity $SOURCE/quality/NetLiba

svn update --set-depth empty $SOURCE/junk
svn update --set-depth empty $SOURCE/junk/monster
svn update --set-depth infinity $SOURCE/junk/monster/yt

svn update --set-depth empty $SOURCE/yweb
svn update --set-depth infinity $SOURCE/yweb/config
svn update --set-depth empty $SOURCE/yweb/robot
svn update --set-depth infinity $SOURCE/yweb/robot/dbscheeme
svn update --set-depth empty $SOURCE/yweb/robot/mirror
svn update --set-depth infinity $SOURCE/yweb/robot/mirror/libmdb

svn update --set-depth empty $SOURCE/ysite
svn update --set-depth empty $SOURCE/ysite/yandex
svn update --set-depth infinity $SOURCE/ysite/yandex/tarc

# Run cmake, alas.
(cd $BUILD && cmake -DCMAKE_BUILD_TYPE=Debug -DMAKE_ONLY=junk/monster/yt $SOURCE)

