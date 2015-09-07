#!/bin/bash -eux

clean() {
    rm -rf docs/ yt/wrapper/tests.sandbox/*
    python setup.py clean
    sudo make -f debian/rules clean
}

PACKAGE=$1

# Copy package files to the python root
# NB: Symbolic links doesn't work correctly with `sdist upload`
cp -r $PACKAGE/debian $PACKAGE/setup.py .
if [ -f "$PACKAGE/MANIFEST.in" ]; then
    cp $PACKAGE/MANIFEST.in .
fi

# Initial cleanup
clean

# Build debian package
DEB=1 python setup.py sdist --dist-dir=../
DEB=1 dpkg-buildpackage -i -I -rfakeroot

# Upload debian package
REPOS=""
case $PACKAGE in
    yandex-yt-python|yandex-yt-python-tools)
        REPOS="common yt-common"
        ;;
    yandex-yt-transfer-manager|yandex-yt-python-fennel)
        REPOS="yt-common"
        ;;
    yandex-yt-python-yson)
        REPOS="yandex-$(lsb_release --short --codename)"
        ;;
esac

if [ -n "$REPOS" ]; then
    for REPO in $REPOS; do
        VERSION=$(dpkg-parsechangelog | grep Version | awk '{print $2}')
        dupload "../${PACKAGE}_${VERSION}_amd64.changes" --force --to $REPO
    done

    # Upload python wheel
    python setup.py bdist_wheel upload -r yandex
fi

# Some postprocess steps
if [ -f "$PACKAGE/postprocess.sh" ]; then
    $PACKAGE/postprocess.sh
fi


# Final cleanup
clean
rm -rf debian setup.py MANIFEST.in
