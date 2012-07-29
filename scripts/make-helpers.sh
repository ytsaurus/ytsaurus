#!/bin/bash

function get_current_branch()
{
    git branch 2> /dev/null | grep -e "^*" | cut -c 3-
}

function check_current_branch()
{
    local branch="$1"

    [[ "$(get_current_branch)" != "$branch" ]] \
        && echo "*** You have to be on a $branch branch." \
        && exit 1
}

function check_current_directory()
{
    [[ ! -d ./yt || ! -d ./yt/ytlib ]] \
        && echo "*** You have to be in a source root." \
        && exit 1
    [[ ! -f cmake/Version.cmake ]] \
        && echo "*** cmake/Version.cmake does not exist; WTF?" \
        && exit 1
    [[ ! -f debian/changelog ]] \
        && echo "*** debian/changelog does not exist; WTF?" \
        && exit 1
}

function get_current_major()
{
    cat cmake/Version.cmake \
        | egrep '^set\(YT_VERSION_MAJOR [0-9]+\)$' \
        | tr '()' '  ' \
        | awk '{print $3}'
}

function get_current_minor()
{
    cat cmake/Version.cmake \
        | egrep '^set\(YT_VERSION_MINOR [0-9]+\)$' \
        | tr '()' '  ' \
        | awk '{print $3}'
}

function get_current_patch()
{
    cat cmake/Version.cmake \
        | egrep '^set\(YT_VERSION_PATCH [0-9]+\)$' \
        | tr '()' '  ' \
        | awk '{print $3}'
}

function update_cmakelists()
{
    local major="$1" ; shift
    local minor="$1" ; shift
    local patch="$1" ; shift

    echo "*** Updating cmake/Version.cmake"

    local temporary=$(mktemp)
    cp cmake/Version.cmake  $temporary
    cat $temporary | awk "
    /^set\\(YT_VERSION_MAJOR/ { print \"set(YT_VERSION_MAJOR ${major})\" ; next }
    /^set\\(YT_VERSION_MINOR/ { print \"set(YT_VERSION_MINOR ${minor})\" ; next }
    /^set\\(YT_VERSION_PATCH/ { print \"set(YT_VERSION_PATCH ${patch})\" ; next }
    { print }
    " > cmake/Version.cmake && rm -f $temporary
}

function update_debian_changelog()
{
    local major="$1" ; shift
    local minor="$1" ; shift
    local patch="$1" ; shift

    echo "*** Updating debian/changelog"

    dch \
        --distributor "yandex" \
        --distribution "unstable" \
        --newversion "${major}.${minor}.${patch}" \
        --urgency "high" \
        --force-distribution \
        "$@"
}

