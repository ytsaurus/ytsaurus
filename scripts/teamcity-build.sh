#!/bin/bash
################################################################################

function shout() {
    echo '*' $@ >&2
}

function tc() {
    echo "##teamcity[$*]"
}

function usage() {
    echo "$0: effortlessly build yt source tree on TeamCity build farm"
    echo "$0: <checkout-directory> <working-directory> <build-type> <build-package>"
    echo ""
    echo "<build-type> have to be compliant with CMAKE_BUILD_TYPE"
    echo "<build-package> have to be either YES or NO"
    echo ""
    echo "Following environment variables must be set:"
    echo "  TEAMCITY_VERSION"
    echo "  TEAMCITY_BUILDCONF_NAME"
    echo "  TEAMCITY_PROJECT_NAME"
    echo "  BUILD_NUMBER"
    echo "  BUILD_VCS_NUMBER"
}

################################################################################
tc "progressMessage 'Setting up...'"

[[ -z "$TEAMCITY_VERSION"        ]] && usage && exit 1
[[ -z "$TEAMCITY_BUILDCONF_NAME" ]] && usage && exit 1
[[ -z "$TEAMCITY_PROJECT_NAME"   ]] && usage && exit 1

[[ -z "$BUILD_NUMBER" ]] && usage && exit 2
[[ -z "$BUILD_VCS_NUMBER" ]] && usage && exit 2

[[ -z "$1" || -z "$2" || -z "$3" || -z "$4" ]] && usage && exit 3

CHECKOUT_DIRECTORY=$1
WORKING_DIRECTORY=$2
BUILD_TYPE=$3
BUILD_PACKAGE=$4

if [[ ( $BUILD_PACKAGE != "YES" ) && ( $BUILD_PACKAGE != "NO" ) ]]; then
    shout "BUILD_PACKAGE have to be either YES or NO."
    exit 1
fi

if [[ -z "$CC" ]]; then
    shout "C compiler is not specified; trying to find gcc-4.5..."
    CC=$(which gcc-4.5)
    shout "CC=$CC"
fi

if [[ -z "$CXX" ]]; then
    shout "C++ compiler is not specified; trying to find g++-4.5..."
    CXX=$(which g++-4.5)
    shout "CXX=$CXX"
fi

################################################################################

export CC
export CXX

export LC_ALL=C
export LANG=en_US.UTF-8

set -e
set -x

################################################################################

mkdir -p $WORKING_DIRECTORY

cd $WORKING_DIRECTORY

tc "blockOpened name='CMake'"

shout "Running CMake..."
tc "progressMessage 'Running CMake...'"
cmake \
    -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
    -DCMAKE_COLOR_MAKEFILE:BOOL=OFF \
    -DYT_BUILD_ENABLE_EXPERIMENTS:BOOL=ON \
    -DYT_BUILD_ENABLE_TESTS:BOOL=ON \
    -DYT_BUILD_NUMBER=$BUILD_NUMBER \
    -DYT_BUILD_TAG=$(echo $BUILD_VCS_NUMBER | cut -c 1-7) \
    $CHECKOUT_DIRECTORY

#trap '(cd $WORKING_DIRECTORY ; make clean ; find . -name "default.log" -delete)' 0

tc "blockClosed name='CMake'"

if [[ $BUILD_PACKAGE = "YES" ]]; then
    tc "blockOpened name='Package'"
    tc "progressMessage 'Building package...'"

    cmake \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DCMAKE_COLOR_MAKEFILE:BOOL=OFF \
        -DYT_BUILD_ENABLE_EXPERIMENTS:BOOL=OFF \
        -DYT_BUILD_ENABLE_TESTS:BOOL=OFF \
        .

    make package && dupload --to common --nomail ARTIFACTS/*.changes

    tc "blockClosed name='Package'"
fi

tc "blockOpened name='make'"

shout "Running make (1/2; fast)..."
tc "progressMessage 'Running make (1/2; fast)...'"
make -j 8 >/dev/null 2>/dev/null || true

shout "Running make (2/2; slow)..."
tc "progressMessage 'Running make (2/2; slow)...'"
make -j 1

tc "blockClosed name='make'"

set +e
a=0

tc "blockOpened name='Unit Tests'"

shout "Running unit tests..."
tc "progressMessage 'Running unit tests...'"

cd $WORKING_DIRECTORY
gdb \
    --batch \
    --return-child-result \
    --command=$CHECKOUT_DIRECTORY/scripts/teamcity-gdb-script \
    --args \
    ./bin/unittester \
        --gtest_color=no \
        --gtest_output=xml:$WORKING_DIRECTORY/test_unit.xml
b=$?
a=$((a+b))

tc "blockClosed name='Unit Tests'"

tc "blockOpened name='Integration Tests'"

shout "Running integration tests..."
tc "progressMessage 'Running integration tests...'"

cd $CHECKOUT_DIRECTORY/scripts/testing
PATH=$WORKING_DIRECTORY/bin:$PATH \
    py.test \
        -rxs -v \
        --assert=plain \
        --junitxml=$WORKING_DIRECTORY/test_integration.xml
b=$?
a=$((a+b))

tc "blockClosed name='Integration Tests'"

cd $WORKING_DIRECTORY

# TODO(sandello): Export final package name as build parameter.
# TODO(sandello): Measure some statistics.
exit $a

