#!/bin/bash -eux

PACKAGE=$1

rm -rf debian setup.py

ln -s $PACKAGE/debian debian
ln -s $PACKAGE/setup.py setup.py

python setup.py clean
sudo make -f debian/rules clean

YT="yt/wrapper/yt"
VERSION=$(dpkg-parsechangelog | grep Version | awk '{print $2}')
if [ "$PACKAGE" = "yandex-yt-python" ]; then
    make version
    make docs
fi

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
dupload "../${PACKAGE}_${VERSION}_amd64.changes" --to $REPO

if [ "$PACKAGE" = "yandex-yt-python" ]; then
    # Upload egg
    export YT_PROXY=kant.yt.yandex.net
    DEST="//home/files"

    make egg
    EGG_VERSION=$(echo $VERSION | tr '-' '_')
    eggname="YandexYt-${EGG_VERSION}_-py2.7.egg"
    cat dist/$eggname | $YT upload "$DEST/YandexYt-${VERSION}-py2.7.egg"
    cat dist/$eggname | $YT upload "$DEST/YandexYt-py2.7.egg"
    
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
rm -rf debian setup.py
