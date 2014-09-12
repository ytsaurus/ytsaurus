#!/bin/bash -eux

PACKAGE=$1

rm -rf debian setup.py docs

# NB: Symbolic links doesn't work correctly with `sdist upload`
cp -r $PACKAGE/debian $PACKAGE/setup.py .
if [ -f "$PACKAGE/MANIFEST.in" ]; then
    cp $PACKAGE/MANIFEST.in .
fi

python setup.py clean
sudo make -f debian/rules clean

# Build and upload package
make deb_without_test

if [ "$PACKAGE" = "yandex-yt-python-yson" ]; then
    REPO="precise"
else
    if [ "$PACKAGE" = "yandex-yt-python-fennel" ] || [ "$PACKAGE" = "yandex-yt-transfer-manager" ]; then
        REPO="yt-common"
    else
        REPO="common"
    fi
fi

VERSION=$(dpkg-parsechangelog | grep Version | awk '{print $2}')
dupload "../${PACKAGE}_${VERSION}_amd64.changes" --to $REPO

if [ "$PACKAGE" = "yandex-yt-python-yson" ]; then
    python setup.py bdist_wheel upload -r yandex
fi

if [ "$PACKAGE" = "yandex-yt-python" ]; then
    # Upload egg
    export YT_PROXY=kant.yt.yandex.net
    DEST="//home/files"

    python setup.py bdist_wheel upload -r yandex

    YT="yt/wrapper/yt"

    make egg
    EGG_VERSION=$(echo $VERSION | tr '-' '_')
    eggname="yandex_yt-${EGG_VERSION}-py2.7.egg"
    cat dist/$eggname | $YT upload "$DEST/yandex_yt-${VERSION}-py2.7.egg"
    cat dist/$eggname | $YT upload "$DEST/yandex_yt-py2.7.egg"
    
    # Upload self-contained binaries
    mv yt/wrapper/pickling.py pickling.py
    cp standard_pickling.py yt/wrapper/pickling.py
    
    for name in yt mapreduce-yt; do
        rm -rf build dist
        pyinstaller/pyinstaller.py --noconfirm --onefile yt/wrapper/$name
        pyinstaller/pyinstaller.py --noconfirm "${name}.spec"
        cat dist/$name | $YT upload "$DEST/${name}_${VERSION}"
        cat dist/$name | $YT upload "$DEST/$name"
    done
    
    mv pickling.py yt/wrapper/pickling.py
fi

python setup.py clean
sudo make -f debian/rules clean
rm -rf debian setup.py VERSION MANIFEST.in
