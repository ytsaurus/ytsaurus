#!/bin/bash -ex

if [ -n "$1" ]; then
    VERSION="$1"
else
    VERSION="$(dpkg-parsechangelog | grep Version | awk '{print $2}')"
fi

TMP_DIR="$(pwd)/tmp_dir"
rm -rf "$TMP_DIR"
mkdir "$TMP_DIR"

pushd "$TMP_DIR"

mkdir arcadia_python_yt

svn checkout "svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia/contrib/python/yt_trunk/" "arcadia_python_yt"

ARCADIA="arcadia_python_yt/$VERSION"

if [ -e "$ARCADIA" ]; then
    echo "Version $VERSION is already commited to arcadia" >&2
    exit 0
fi


mkdir "$ARCADIA"

apt-get download "yandex-yt-python=$VERSION"

dpkg -x "yandex-yt-python_${VERSION}_all.deb" ${VERSION}

cp -r "${VERSION}/usr/share/pyshared/yt" "$ARCADIA/"
cp -r "${VERSION}/usr/bin" "$ARCADIA/"

pushd "arcadia_python_yt/" 
rm -rf latest
cp -r "$VERSION" latest 
svn add *
svn ci -m "Updated contrib/python/yt_trunk to version $VERSION"
popd

popd

rm -rf "$TMP_DIR"
