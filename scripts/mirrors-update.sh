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

for repo in $MIRRORS_DIRECTORY/github-*.git ; do
    echo "=== $repo"
    (cd $repo && git fetch github)
    (cd $repo && git branch -f master $(git show-ref -s refs/upstream/heads/master))
    (cd $repo && git push origin)
done

