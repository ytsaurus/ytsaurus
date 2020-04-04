#!/bin/bash

MIRRORS_DIRECTORY="$HOME/mirrors"

set -x
set -e

# git-svn may mangle commit messages when running with improper encoding.
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8

################################################################################

SVN_LWS=100000

sync() {
    local repo="$1"
    echo "=== $repo"
    # Carefully work around "Restored trunk" commits in SVN as git-svn is incapable of
    # tracking child-parent relation between these.
    (cd $repo && git svn fetch --log-window-size $SVN_LWS --revision 0:174921)
    (cd $repo && git svn fetch --log-window-size $SVN_LWS --revision 174921:174922)
    (cd $repo && git svn fetch --log-window-size $SVN_LWS --revision 174922:907136)
    (cd $repo && git svn fetch --log-window-size $SVN_LWS --revision 907136:907137)
    (cd $repo && git svn fetch --log-window-size $SVN_LWS --revision 907137:2359112)
    (cd $repo && git svn fetch --log-window-size $SVN_LWS --revision 2359112:2359113)
    (cd $repo && git svn fetch --log-window-size $SVN_LWS --revision 2359113:HEAD)
    (cd $repo && git branch -f master $(git show-ref -s refs/remotes/git-svn))
    (cd $repo && git push origin)
}

for repo in $MIRRORS_DIRECTORY/arcadia-*.git ; do
    sync "$repo" &
done
wait

