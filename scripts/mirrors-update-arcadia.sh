#!/bin/bash

MIRRORS_DIRECTORY="$HOME/mirrors"

set -x
set -e

################################################################################

for repo in $MIRRORS_DIRECTORY/arcadia-*.git ; do
    echo "=== $repo"
    (cd $repo && git svn fetch)
    (cd $repo && git branch -f master $(git show-ref -s refs/remotes/git-svn))
    (cd $repo && git push origin)
done

