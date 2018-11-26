#!/bin/bash -ex

die() {
    echo $@
    exit 1
}

usage() {
    echo "Usage: $0 {package_name} {arcadia_path} [{version}]"
}

[ -n "$1" ] || (usage && die)
PACKAGE_NAME="$1" && shift

[ -n "$1" ] || (usage && die)
ARCADIA_PATH="$1" && shift

pushd "$PACKAGE_NAME"

if [ -n "$1" ]; then
    VERSION="$1"
else
    VERSION="$(dpkg-parsechangelog | grep '^Version' | awk '{print $2}')"
fi

if [ -n "$ARCADIA_DIR" ]; then
    SPECIFIED_ARCADIA_DIR="1"
    ARCADIA_DIR="$ARCADIA_DIR/$ARCADIA_PATH"
else
    SPECIFIED_ARCADIA_DIR="0"
    ARCADIA_DIR="arcadia_dir"
fi

if [ -z "$SVN" ]; then
    SVN="svn"
fi

TMP_DIR="$(pwd)/tmp_dir"
rm -rf "$TMP_DIR" && mkdir "$TMP_DIR"

pushd "$TMP_DIR"

mkdir -p "$ARCADIA_DIR"

if [ "$SPECIFIED_ARCADIA_DIR" = "0" ]; then
    $SVN checkout "svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia/$ARCADIA_PATH/" "$ARCADIA_DIR"
fi

ARCADIA="$ARCADIA_DIR/$VERSION"

if [ -e "$ARCADIA" ]; then
    echo "Version $VERSION is already commited to arcadia" >&2
    exit 0
fi

mkdir -p "$ARCADIA"

apt-get download "${PACKAGE_NAME}=${VERSION}"

dpkg -x "${PACKAGE_NAME}_${VERSION}_all.deb" "${VERSION}"

cp -r "${VERSION}/usr/share/pyshared/"* "$ARCADIA/"
rm -rf "$ARCADIA/*egg-info"
cp -r "${VERSION}/usr/bin" "$ARCADIA/"

pushd "$ARCADIA_DIR"

mkdir -p latest
pushd latest
LATEST_FILES="$(find . -name "*")"
popd

for file in $LATEST_FILES; do
    if [ ! -e "$VERSION/$file" -a -e "$LATEST/$file" ]; then
        $SVN rm "$LATEST/$file"
    fi
done
cp -r "$VERSION"/* latest
$SVN add latest --force
#svn add "$VERSION"
if [ "$SPECIFIED_ARCADIA_DIR" = "0" ]; then
    echo $SVN ci -m "Updated $ARCADIA_PATH to version $VERSION"
else
    echo "Make checks and then manually commit with message \"Updated $ARCADIA_PATH to version $VERSION\""
fi

popd # $ARCADIA_DIR

popd # $TMP_DIR

rm -rf "$TMP_DIR"

popd # $PACKAGE_NAME
