#!/bin/bash -eux
# Build and upload self-contained binaries

export YT_PROXY=kant.yt.yandex.net
export YT_TOKEN=1da6afc98d189e8ba59d2ea39f29d0f1 #teamcity user
DEST="//home/files"
YT="yt/wrapper/yt"

# Upload self-contained binaries
mv yt/wrapper/pickling.py pickling.py
cp standard_pickling.py yt/wrapper/pickling.py

for name in yt mapreduce-yt; do
    rm -rf build dist
    pyinstaller/pyinstaller.py --noconfirm --onefile yt/wrapper/$name
    pyinstaller/pyinstaller.py --noconfirm "${name}.spec"
    VERSION=$(dpkg-parsechangelog | grep Version | awk '{print $2}')
    cat dist/$name | $YT upload "$DEST/${name}_${VERSION}"
    cat dist/$name | $YT upload "$DEST/$name"
done

mv pickling.py yt/wrapper/pickling.py
