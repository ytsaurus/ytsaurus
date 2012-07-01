#!/bin/bash

DIRECTORY="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${DIRECTORY}/make-helpers.sh

check_current_directory
check_current_branch stable

major=$(get_current_major)
minor=$(get_current_minor)
patch=$(get_current_patch)

################################################################################
[[ -z "$1" ]] \
    && echo "Please, specify a short hotfix description, like:" \
    && echo "   chunk-replication" \
    && echo "   json-not-verified-in-http-proxy" \
    && echo "   stupid-bug-in-table-writer" \
    && exit 1

cause="$1"

major=${major}
minor=${minor}
patch=$((${patch} + 1))
version="${major}.${minor}.${patch}"

echo "*** Current version is '${major}.${minor}.${version}'"
echo "*** New version is '${major}.${minor}.${version}'"

git flow hotfix start "${cause}"

update_cmakelists $major $minor $patch
update_debian_changelog $major $minor $patch "Hotfix '${cause}'"

git add 'CMakeLists.txt'
git add 'debian/changelog'
git commit -m "Version bump; hotfix ${version} (${cause})"

echo "*** Now you can safely do your fixing!"
echo "*** Please, finish the hotfix with the following command:"
echo "\n    git flow hotfix finish ${cause}\n"
