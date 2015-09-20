#!/bin/bash -eux

YANDEX_YT_VERSION="0.17.2-stable-without-yt~5548+14b624c"
YANDEX_YT_PYTHON_VERSION="0.6.43-0"
YANDEX_YT_LOCAL_VERSION="0.0.10-0"

download_and_extract() {
    local package="$1"
    local version="$2"
    apt-get download "$package=$version"
    dpkg -x ${package}_*.deb $package
}

TMP_DIR="$(mktemp -d /tmp/$(basename $0).XXXXXX)"
cd "$TMP_DIR"

download_and_extract yandex-yt $YANDEX_YT_VERSION
download_and_extract yandex-yt-http-proxy $YANDEX_YT_VERSION
download_and_extract yandex-yt-python $YANDEX_YT_PYTHON_VERSION
download_and_extract yandex-yt-local $YANDEX_YT_LOCAL_VERSION

mkdir -p "archive/bin"
mkdir -p "archive/python/yt"
cp -r yandex-yt/usr/bin/ytserver archive/bin/
cp -r yandex-yt-http-proxy/usr/lib/node_modules archive/
cp -r yandex-yt-python/usr/share/pyshared/yt/* archive/python/yt
cp -r yandex-yt-python/usr/bin/yt2 archive/bin/
cp -r yandex-yt-local/usr/share/pyshared/yt/* archive/python/yt
cp -r yandex-yt-local/usr/bin/yt_local archive/bin/

tar cvf archive.tgz archive/ 
cat archive.tgz | yt2 upload //home/files/yt_local_archive.tgz --proxy locke

rm -rf "$TMP_DIR"


