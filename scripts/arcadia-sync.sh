#!/bin/bash

msg() {
    echo "*** $*"
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

# Options.
force=
review=
abi=

while getopts "frv:" opt; do
    case "$opt" in
    f)
        force=y
        ;;
    r)
        review=y
        ;;
    v)
        abi="$OPTARG"
        ;;
    \?)
        die "Invalid option: -$OPTARG" >&2
        ;;
    :)
        die "Missing required argument: -$OPTARG" >&2
        ;;
    esac
done

shift "$((OPTIND - 1))"

arc="svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia/yt"
cmd="$1"
if [[ ! "$cmd" =~ ^init|pull|push$ ]]; then
    die "USAGE: $0: [-f] [-r] [-v VERSION] {init|pull|push}"
fi

_detect_abi() {
    local ref=$(git rev-parse --abbrev-ref HEAD)
    if ! echo "$ref" | grep -Eq "^(pre)?stable/" 2>&1; then
        die "Current branch must be either 'prestable/' or 'stable/'"
    fi
    abi=${ref#stable/}
    abi=${abi#prestable/}
    abi=${abi#0.}
    abi=${abi/./_}
}

_check_abi() {
    if ! svn info "${arc}/${abi}" >/dev/null 2>&1; then
        die "Missing appropriate branch in SVN"
    fi
    arc="${arc}/${abi}/yt"
    arc_remote="arcadia_svn_${abi}"
}

_ensure_clean() {
    if ! git diff-index HEAD --exit-code --quiet 2>&1; then
        die "Working tree has local modifications"
    fi
    if ! git diff-index --cached HEAD --exit-code --quiet 2>&1; then
        die "Index has local modifications"
    fi
}

_ensure_up_to_date() {
    if [ "$(git version)" \< "git version 1.7" ]; then
        die "git >= 1.8 is required"
    fi
}

_fetch_svn_remote() {
    local lws=100000
    # work around problematic revisions
    set -x
    git svn --svn-remote "$arc_remote" fetch --log-window-size ${lws} --revision 0:174921
    git svn --svn-remote "$arc_remote" fetch --log-window-size ${lws} --revision 174921:174922
    git svn --svn-remote "$arc_remote" fetch --log-window-size ${lws} --revision 174922:907136
    git svn --svn-remote "$arc_remote" fetch --log-window-size ${lws} --revision 907136:907137
    git svn --svn-remote "$arc_remote" fetch --log-window-size ${lws} --revision 907137:2359112
    git svn --svn-remote "$arc_remote" fetch --log-window-size ${lws} --revision 2359112:2359113
    git svn --svn-remote "$arc_remote" fetch --log-window-size ${lws} --revision 2359113:HEAD
    set +x
}

_find_most_recent_sync() {
    local push_commit=""
    local base_commit=""

    git log \
        --grep="^yt:git_commit:" \
        --grep="^Push yt/" \
        --all-match \
        --pretty="format:BEGIN %H%n%s%n%n%b%nEND%n" \
        "refs/remotes/${arc_remote}" | while read a b
    do
        case "$a" in
        BEGIN)
            push_commit="$b"
            ;;
        yt:git_commit:*)
            base_commit="$(echo "$a" | cut -d : -f 3)"
            ;;
        END)
            if [[ "$push_commit" != "" ]]; then
                if [[ "$base_commit" != "" ]]; then
                    echo "$push_commit" "$base_commit"
                    break
                fi
            fi
            push_commit=""
            base_commit=""
        esac
    done
}

do_init() {
    msg "Setting up git-svn..."

    git config --local "svn-remote.${arc_remote}.url" "$arc"
    git config --local "svn-remote.${arc_remote}.fetch" ":refs/remotes/${arc_remote}"

    _fetch_svn_remote

    msg "Done!"
}

do_pull() {
    if [[ $(git rev-parse --verify "arcadia") ]]; then
        die "Branch 'arcadia' already exists; merge it or remove it aforehead"
    fi

    msg "Pulling all remote changes onto HEAD..."

    _fetch_svn_remote

    msg "Searching for most recent push commit..."

    set +x
    local most_recent_sync=$(_find_most_recent_sync)
    if [[ -z "$most_recent_sync" ]]; then
        die "Unable to find most recent synchronization point"
    fi
    set -x

    set $most_recent_sync
    local push_commit="$1"
    local base_commit="$2"

    msg "Found push commit $push_commit based on $base_commit"

    local git_svn_commit=$(git rev-parse "refs/remotes/${arc_remote}")
    local svn_revision=$(git svn --svn-remote "$arc_remote" find-rev "refs/remotes/${arc_remote}")

    set +x
    message="Pull yt/$abi/ from Arcadia"$'\n'
    message="$message"$'\n'"yt:git_svn_commit:$git_svn_commit"
    message="$message"$'\n'"yt:svn_revision:$svn_revision"
    set -x

    # In general, situation looks like this.
    #
    #   mainline  --(B)----(*)----(L)--..--HEAD
    #                 \           /
    #    arcadia  ----(P)--(*)--(*)--
    #
    # or
    #
    #   mainline  ----(L)--(*)--(B)--
    #                 /           \
    #    arcadia  --(*)----(*)----(P)--
    #
    # Here P is the latest push commit with base B, L is the latest pull commit,
    # and we need to figure out how to properly merge Arcadia into mainline.
    # To do this, we test whether merge base of two branches is ancestor of P.

    local merge_base=$(git merge-base HEAD "refs/remotes/${arc_remote}")

    msg "Merge base is $merge_base"

    if git merge-base --is-ancestor "$merge_base" "$push_commit" ; then
        msg "Merge base preceedes latest push; merging via temporary branch."
        git branch "arcadia" "$base_commit"
    else
        msg "Merge base succeedes latest push; merging directly."
        git branch "arcadia" "HEAD"
    fi

    local branch=$(git symbolic-ref --short HEAD)
    git checkout "arcadia"
    git merge -Xsubtree=yt "refs/remotes/${arc_remote}" -m "$message"
    git checkout "$branch"
    git merge "arcadia"

    msg "Merged Arcadia to HEAD!"
    msg ""
    msg "\$ git diff arcadia^..arcadia"
}

do_push() {
    msg "Pulling all remote changes onto HEAD..."

    _fetch_svn_remote

    msg "Pushing changes to Arcadia..."

    local local_tree=$(git write-tree --prefix=yt/)
    local remote_tree=$(git cat-file -p "refs/remotes/${arc_remote}" | grep '^tree' | awk '{print $2}')
    local git_commit=$(git rev-parse HEAD)
    local last_git_svn_commit=$(git rev-parse "refs/remotes/${arc_remote}")
    local last_svn_revision=$(git svn --svn-remote "$arc_remote" find-rev "refs/remotes/${arc_remote}")

    msg "Committing difference between local tree $local_tree and remote tree $remote_tree..."

    set +x
    message="Push yt/$abi/ to Arcadia"$'\n'
    if [[ "$force" == "y" ]]; then
        message="$message"$'\n'"__FORCE_COMMIT__"$'\n'
    else
        message="$message"$'\n'"__BYPASS_CHECKS__"$'\n'
    fi
    if [[ "$review" == "y" ]]; then
        message="$message"$'\n'"REVIEW: NEW"$'\n'
    fi
    message="$message"$'\n'"yt:git_commit:$git_commit"
    message="$message"$'\n'"yt:last_git_svn_commit:$last_git_svn_commit"
    message="$message"$'\n'"yt:last_svn_revision:$last_svn_revision"
    set -x

    git svn --svn-remote "$arc_remote" commit-diff -r "$last_svn_revision" -m "$message" "$remote_tree" "$local_tree" "$arc"
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
