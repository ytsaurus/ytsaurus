#!/bin/bash

MIRRORS_DIRECTORY="$HOME/mirrors"

set -x
set -e

################################################################################

for repo in $MIRRORS_DIRECTORY/github-*.git ; do
    echo "=== $repo"
    (cd $repo && git fetch github)
    (cd $repo && git branch -f upstream/HEAD $(git show-ref -s refs/upstream/HEAD))
    (cd $repo && git push origin)
done

