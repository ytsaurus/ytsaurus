#!/bin/bash

DIRECTORY="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${DIRECTORY}/make-helpers.sh

check_current_directory
#check_current_branch stable

major=$(get_current_major)
minor=$(get_current_minor)
patch=$(get_current_patch)

################################################################################
if [[ -z "$1" ]]; then
    if $(get_current_branch | grep -q "^hotfix/"); then
        version="${major}.${minor}.${patch}"
        git flow hotfix finish -s -m 'Happily brought to you by ./make-release.sh' ${version}
    fi
fi

[[ -z "$1" ]] \
    && echo "Please, specify a short hotfix description, like:" \
    && echo "   fix-chunk-replication" \
    && echo "   json-not-verified-in-http-proxy" \
    && echo "   stupid-bug-in-table-writer" \
    && exit 1

cause="$1"

echo "*** Current version is '${major}.${minor}.${patch}'"

major=${major}
minor=${minor}
patch=$((${patch} + 1))
version="${major}.${minor}.${patch}"

echo "*** New version is '${major}.${minor}.${patch}'"

git flow hotfix start "${version}"

update_cmakelists $major $minor $patch
update_debian_changelog $major $minor $patch "Hotfix '${cause}'"

git add 'CMakeLists.txt'
git add 'debian/changelog'
git commit -m "Version bump; hotfix ${version} (${cause})"

echo "*** Now you can safely do your fixing!"
echo "*** Run this script again to finish fixing."
