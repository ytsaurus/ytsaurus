#!/bin/bash

msg() {
    echo "$*"
}

die() {
    echo "$*" >&2
    exit 1
}

# Options.
force=
abi=

while getopts "fv:" opt; do
    case "$opt" in
    f)
        force=y
        ;;
    v)
        abi="$OPTARG"
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

arc="svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia/yt"
cmd="$1"
if [[ ! "$cmd" =~ ^init|pull|push$ ]]; then
    die "USAGE: $0: [-f] [-v VERSION] {init|pull|push}"
fi

_detect_abi() {
    local ref=$(git rev-parse --abbrev-ref HEAD)
    if ! echo "$ref" | grep -Eq "^(pre)?stable/" 2>&1; then
        die "ERROR: Current branch must be either 'prestable/' or 'stable/'"
    fi
    abi=${ref#stable/}
    abi=${abi#prestable/}
    abi=${abi#0.}
    abi=${abi/./_}
}

_check_abi() {
    if ! svn info "${arc}/${abi}" >/dev/null 2>&1; then
        die "ERROR: Missing appropriate branch in SVN."
    fi
    arc="${arc}/${abi}/yt"
    arc_remote="arcadia_svn_${abi}"
}

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
    git svn --svn-remote "$arc_remote" fetch --log-window-size 100000
}

do_init() {
    echo "*** Setting up git-svn..."

    git config --local "svn-remote.${arc_remote}.url" "$arc"
    git config --local "svn-remote.${arc_remote}.fetch" ":refs/remotes/${arc_remote}"

    _fetch_svn_remote
}

do_pull() {
    echo "*** Pulling all remote changes onto HEAD..."

    _fetch_svn_remote

    git_commit=$(git rev-parse HEAD)
    git_svn_commit=$(git rev-parse "refs/remotes/${arc_remote}")
    svn_revision=$(git svn find-rev "refs/remotes/${arc_remote}")

    set +x
    message="Pull yt/$abi/ from Arcadia"$'\n'
    message="$message"$'\n'"yt:git_commit:$git_commit"
    message="$message"$'\n'"yt:git_svn_commit:$git_svn_commit"
    message="$message"$'\n'"yt:svn_revision:$svn_revision"
    set -x

    git merge -Xsubtree=yt "refs/remotes/${arc_remote}" -m "$message"
}

do_push() {
    echo "*** Pushing changes to remote..."

    _fetch_svn_remote

    local_tree=$(git write-tree --prefix=yt/)
    remote_tree=$(git cat-file -p "refs/remotes/${arc_remote}" | grep '^tree' | awk '{print $2}')
    git_commit=$(git rev-parse HEAD)
    git_svn_commit=$(git rev-parse "refs/remotes/${arc_remote}")
    svn_revision=$(git svn find-rev "refs/remotes/${arc_remote}")

    set +x
    message="Push yt/$abi/ to Arcadia"$'\n'
    if [[ "$force" == "y" ]]; then
        message="$message"$'\n'"__FORCE_COMMIT__"$'\n'
    else
        message="$message"$'\n'"__BYPASS_CHECKS__"$'\n'
    fi
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

_detect_abi
_check_abi

msg "ABI: '$abi'"
msg "ARC: '$arc'"

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
