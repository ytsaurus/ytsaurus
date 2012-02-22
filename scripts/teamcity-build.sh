#!/bin/bash
################################################################################

function usage() {
    echo "$0: effortlessly build yt source tree on TeamCity build farm"
    echo "$0: <checkout-directory> <working-directory>"
    echo ""
    echo "Following environment variables must be set:"
    echo "  TEAMCITY_VERSION"
    echo "  TEAMCITY_BUILDCONF_NAME"
    echo "  TEAMCITY_PROJECT_NAME"
    echo "  BUILD_NUMBER"
    echo "  BUILD_TYPE"
}

################################################################################

[[ -z "$TEAMCITY_VERSION"        ]] && usage && exit 1
[[ -z "$TEAMCITY_BUILDCONF_NAME" ]] && usage && exit 1
[[ -z "$TEAMCITY_PROJECT_NAME"   ]] && usage && exit 1

[[ -z "$BUILD_NUMBER" || -z "$BUILD_TYPE" ]] && usage && exit 2
[[ -z "$1" || -z "$2" ]] && usage && exit 3

CHECKOUT_DIRECTORY=$1
WORKING_DIRECTORY=$2

if [[ -z "$CC" ]]; then
    echo "* C compiler is not specified; trying to find gcc-4.5..." >&2
    CC=$(which gcc-4.5)
    echo "* CC=$CC" >&2
fi

if [[ -z "$CXX" ]]; then
    echo "* C++ compiler is not specified; trying to find g++-4.5..." >&2
    CXX=$(which g++-4.5)
    echo "* CXX=$CXX" >&2
fi

################################################################################

export CC
export CXX

export LC_ALL=C
export LANG=en_US.UTF-8

set -e
set -x

################################################################################

cd $WORKING_DIRECTORY

echo "* Running CMake..." >&2
cmake -DCMAKE_BUILD_TYPE=$BUILD_TYPE -DCMAKE_COLOR_MAKEFILE:BOOL=OFF $CHECKOUT_DIRECTORY

echo "* Running make (1/2; fast)..." >&2
make -j 8 >/dev/null 2>/dev/null || true

echo "* Running make (2/2; slow)..." >&2
make -j 1

cd $WORKING_DIRECTORY
gdb --batch --command=$CHECKOUT_DIRECTORY/scripts/teamcity-gdb-script --args \
    ./bin/unittester \
        --gtest_color=no \
        --gtest_output=xml:$WORKING_DIRECTORY/test_unit.xml

cd $CHECKOUT_DIRECTORY/scripts/testing
PATH=$WORKING_DIRECTORY/bin:$PATH \
    py.test \
        -rxs -v \
        --assert=plain \
        --junitxml=$WORKING_DIRECTORY/test_integration.xml
