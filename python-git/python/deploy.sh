#!/bin/bash -eux

YT="yt/wrapper/yt"
DEST="//home/files"
VERSION=$(dpkg-parsechangelog | grep Version | awk '{print $2}')
export YT_PROXY=kant.yt.yandex.net

# Build and upload package
make deb_without_test
dupload "../yandex-yt-python_${VERSION}_amd64.changes"

# Upload egg
make egg
EGG_VERSION=$(echo $VERSION | tr '-' '_')
eggname="Yt-${EGG_VERSION}_-py2.7.egg"
cat dist/$eggname | $YT upload "$DEST/Yt-${VERSION}-py2.7.egg"
cat dist/$eggname | $YT upload "$DEST/Yt-py2.7.egg"

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
