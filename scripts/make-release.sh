#!/bin/bash

DIRECTORY="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${DIRECTORY}/make-helpers.sh

check_current_directory
check_current_branch master

major=$(get_current_major)
minor=$(get_current_minor)
patch=$(get_current_patch)

################################################################################
version="${major}.${minor}.${patch}"
echo "*** Current version is '${version}'"

vtype=$1;shift
vdiff=$1;shift;[[ -z "${vdiff}" ]]&&vdiff=1

case "${vtype}" in
    --major)
        echo "*** Bumping major version"
        major=$((${major} + ${vdiff}))
        minor=0
        patch=0
        ;;
    --minor)
        echo "*** Bumping minor version"
        minor=$((${minor} + ${vdiff}))
        patch=0
        ;;
    --patch)
        echo "*** Bumping patch version"
        patch=$((${patch} + ${vdiff}))
        ;;
    *)
        echo "Don't know what to do; please, specify either --major, --minor or --patch"
        exit 1
esac

################################################################################
version="${major}.${minor}.${patch}"
echo "*** New version is '${version}'"

git flow release start "${version}" "$1"

update_cmakelists $major $minor $patch
update_debian_changelog $major $minor $patch

git add 'cmake/Version.cmake'
git add 'debian/changelog'
git commit -m "Version bump; release ${version}"

git flow release finish -s -m 'Happily brought to you by ./make-release.sh' ${version}
