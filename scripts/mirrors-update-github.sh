#!/bin/bash

MIRRORS_DIRECTORY="$HOME/mirrors"

set -x
set -e

# git-svn may mangle commit messages when running with improper encoding.
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8

################################################################################

sync() {
    local repo="$1"
    echo "=== $repo"
    (cd $repo && git fetch github)
    (cd $repo && git branch -f upstream/HEAD $(git show-ref -s refs/upstream/HEAD))
    (cd $repo && git push origin)
}

for repo in $MIRRORS_DIRECTORY/github-*.git ; do
    sync "$repo" &
done
wait

