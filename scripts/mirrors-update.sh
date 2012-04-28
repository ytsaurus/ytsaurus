#!/bin/bash

MIRRORS_DIRECTORY=/home/sandello/mirrors

set -x
set -e

################################################################################

for repo in $MIRRORS_DIRECTORY/arcadia-*.git ; do
    echo "=== $repo"
    (cd $repo && git svn fetch)
    (cd $repo && git push origin)
done

for repo in $MIRRORS_DIRECTORY/github-*.git ; do
    echo "=== $repo"
    (cd $repo && git fetch github)
    (cd $repo && git push origin)
done

