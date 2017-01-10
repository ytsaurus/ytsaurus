#!/bin/bash -eux

clean() {
    rm -rf yt/wrapper/tests.sandbox/* .pybuild *.egg-info
    python setup.py clean
    sudo make -f debian/rules clean
}

found_version() {
    local package="$1"
    local repo="$2"
    local target_version="$3"
    for branch in "unstable" "testing" "stable"; do
        for version in $(
            curl -s "http://dist.yandex.ru/$repo/$branch/all/Packages.gz" \
                | zcat \
                | grep "Package: $package\$" -A 1 \
                | awk '{if ($1 == "Version:") print $2}'); do
            if [ "$version" = "$target_version" ]; then
                echo 1
                return
            fi
        done
    done
    echo 0
}

init_vars() {
    set +u

    if [ -z "$FORCE_DEPLOY" ]; then
        FORCE_DEPLOY=""
    fi
    export FORCE_DEPLOY

    if [ -z "$FORCE_BUILD" ]; then
        FORCE_BUILD=""
    fi
    export FORCE_BUILD

    if [ -z "$SKIP_WHEEL" ]; then
        SKIP_WHEEL=""
    fi
    export SKIP_WHEEL

    if [ -z "$CREATE_CONDUCTOR_TICKET" ]; then
        CREATE_CONDUCTOR_TICKET=""
    fi
    export CREATE_CONDUCTOR_TICKET

    set -u
}

PACKAGE=$1

init_vars

# Copy package files to the python root
# NB: Symbolic links doesn't work correctly with `sdist upload`
cp -r $PACKAGE/debian $PACKAGE/setup.py .
if [ -f "$PACKAGE/MANIFEST.in" ]; then
    cp $PACKAGE/MANIFEST.in .
fi

# Initial cleanup
clean

VERSION=$(dpkg-parsechangelog | grep Version | awk '{print $2}')

# Detect repos to upload.
REPOS=""
case $PACKAGE in
    yandex-yt-python|yandex-yt-python-tools|yandex-yt-local|yandex-yt-transfer-manager-client)
        REPOS="common yt-common search"
        ;;
    yandex-yt-transfer-manager|yandex-yt-python-fennel|yandex-yt-fennel)
        REPOS="yt-common"
        ;;
    yandex-yt-python-yson)
        REPOS="yandex-$(lsb_release --short --codename) yt-$(lsb_release --short --codename)"
        ;;
esac

REPOS_TO_UPLOAD=""
if [ -z "$FORCE_DEPLOY" ]; then
    for REPO in $REPOS; do
        if [ "$(found_version "$PACKAGE" "$REPO" "$VERSION")" = "0" ]; then
            REPOS_TO_UPLOAD="$REPOS_TO_UPLOAD $REPO"
        fi
    done
else
    REPOS_TO_UPLOAD="$REPOS"
fi

# Build and upload debian package if necessary
if [ -n "$REPOS_TO_UPLOAD" ] || [ -n "$FORCE_BUILD" ]; then
    # NB: Never strip binaries and so-libraries.
    DEB_STRIP_EXCLUDE=".*" DEB=1 dpkg-buildpackage -i -I -rfakeroot

    # Upload debian package
    for REPO in $REPOS_TO_UPLOAD; do
        if [ "$REPO" = "common" ]; then
            # NB: used in postptocess.sh by some packages.
            export CREATE_CONDUCTOR_TICKET="true"
        fi
        dupload "../${PACKAGE}_${VERSION}_amd64.changes" --force --to $REPO
    done
fi

# Upload python wheel
if [ -z "$SKIP_WHEEL" ]; then
    python setup.py bdist_wheel --universal upload -r yandex
fi

# Some postprocess steps
if [ -f "$PACKAGE/postprocess.sh" ]; then
    $PACKAGE/postprocess.sh
fi


# Final cleanup
clean
rm -rf debian setup.py MANIFEST.in
