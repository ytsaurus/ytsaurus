#!/bin/bash

# Options.
force=

while getopts "f" opt; do
    case "$opt" in
    f)
        force=y
        ;;
    \?)
        die "ERROR: Invalid option: -$OPTARG" >&2
        ;;
    :)
        die "ERROR: Missing required argument: -$OPTARG" >&2
        ;;
    esac
done

shift "$((OPTIND - 1))"

cmd=$1
if [[ ! "$cmd" =~ ^init|pull|push$ ]]; then
    echo "USAGE: $0: [-f] {init|pull|push}" >&2
    exit 1
fi

_ensure_clean() {
    if ! git diff-index HEAD --exit-code --quiet 2>&1; then
        die "ERROR: Working tree has modifications."
    fi
    if ! git diff-index --cached HEAD --exit-code --quiet 2>&1; then
        die "ERROR: Index has modifications."
    fi
}

_ensure_up_to_date() {
    if [ "$(git version)" \< "git version 1.7" ]; then
        die "ERROR: git >= 1.8 is required."
    fi
}

_fetch_svn_remote() {
    git svn fetch --log-window-size 100000
}

do_init() {
    echo "*** Setting up git-svn..."
    git config --local svn-remote.svn.url "svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia/yt/"
    git config --local svn-remote.svn.fetch ":refs/remotes/git-svn"
    _fetch_svn_remote
}

do_pull() {
    echo "*** Pulling all remote changes onto HEAD..."
    _fetch_svn_remote

    git_commit=$(git rev-parse HEAD)
    git_svn_commit=$(git rev-parse refs/remotes/git-svn)
    svn_revision=$(git svn find-rev refs/remotes/git-svn)

    set +x
    message="Pull yt/ from Arcadia"$'\n'
    message="$message"$'\n'"yt:git_commit:$git_commit"
    message="$message"$'\n'"yt:git_svn_commit:$git_svn_commit"
    message="$message"$'\n'"yt:svn_revision:$svn_revision"
    set -x

    git merge -Xsubtree=yt refs/remotes/git-svn -m "$message"
}

do_push() {
    echo "*** Pushing changes to remote..."
    _fetch_svn_remote

    local_tree=$(git write-tree --prefix=yt/)
    remote_tree=$(git cat-file -p refs/remotes/git-svn | grep '^tree' | awk '{print $2}')
    git_commit=$(git rev-parse HEAD)
    git_svn_commit=$(git rev-parse refs/remotes/git-svn)
    svn_revision=$(git svn find-rev refs/remotes/git-svn)

    set +x
    message="Push yt/ to Arcadia"$'\n'
    # TODO(sandello): Use force only on-demand.
    message="$message"$'\n'"__FORCE_COMMIT__"$'\n'
    message="$message"$'\n'"yt:local_tree:$local_tree"
    message="$message"$'\n'"yt:remote_tree:$remote_tree"
    message="$message"$'\n'"yt:git_commit:$git_commit"
    message="$message"$'\n'"yt:git_svn_commit:$git_svn_commit"
    message="$message"$'\n'"yt:svn_revision:$svn_revision"
    set -x

    git svn commit-diff -r "$svn_revision" -m "$message" "$remote_tree" "$local_tree"
}

################################################################################

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.."
cd "$root"

_ensure_clean
_ensure_up_to_date

set -ex

case "$cmd" in
init)
    do_init
    ;;
pull)
    do_pull
    ;;
push)
    do_push
    ;;
esac
