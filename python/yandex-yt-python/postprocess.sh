#!/bin/bash -eux
# Build and upload self-contained binaries

export YT_PROXY=locke.yt.yandex.net
export YT_TOKEN=1da6afc98d189e8ba59d2ea39f29d0f1 #teamcity user
DEST="//home/files"
YT="yt/wrapper/yt"
UBUNTU_VERSION="$(lsb_release --short --codename )"

make_link()
{
    src="$1"
    dst="$2"
    # TODO(ignat): make atomic
    $YT remove "$dst" --force
    $YT link "$src" "$dst"
}

# Upload python egg
python setup.py bdist_egg
EGG_FILEPATH=$(find dist/ -name "*.egg" | head -n 1)
EGG_FILE="${EGG_FILEPATH##*/}"
cat "$EGG_FILE" | $YT upload "$DEST/$EGG_FILE"
make_link "$DEST/$EGG_FILE" "$DEST/yandex-yt.egg"

# Upload self-contained binaries
mv yt/wrapper/pickling.py pickling.py
cp standard_pickling.py yt/wrapper/pickling.py

for name in yt mapreduce-yt; do
    rm -rf build dist
    pyinstaller/pyinstaller.py --noconfirm --onefile yt/wrapper/$name
    pyinstaller/pyinstaller.py --noconfirm "${name}.spec"
    VERSION=$(dpkg-parsechangelog | grep Version | awk '{print $2}')
    cat dist/$name | $YT upload "$DEST/${name}_${VERSION}_${UBUNTU_VERSION}"
    make_link "$DEST/${name}_${VERSION}_${UBUNTU_VERSION}" "$DEST/${name}_${UBUNTU_VERSION}"
done

mv pickling.py yt/wrapper/pickling.py
