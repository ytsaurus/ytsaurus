#!/bin/bash -ex
# Upload archive with all necessary stuff to Locke.

export YT_PROXY=locke.yt.yandex.net
export YT_TOKEN=1da6afc98d189e8ba59d2ea39f29d0f1  # Teamcity user

YANDEX_YT_LOCAL_VERSION=$(dpkg-parsechangelog | grep Version | awk '{print $2}')
YANDEX_YT_PYTHON_VERSION="0.6.67-0"
YANDEX_YT_VERSION="0.17.3-stable-without-yt~6586~6be3d00"
YANDEX_YT_YSON_BINDINGS_VERSION="0.2.23-0"

ARCHIVE_NAME="yt_local_${YANDEX_YT_LOCAL_VERSION}_archive.tgz"

download_and_extract() {
    local package="$1"
    if [ -n "$2" ]; then
        version_suffix="=$2"
    else
        version_suffix=""
    fi
    apt-get download "$package$version_suffix"
    dpkg -x ${package}_*.deb $package
}

TMP_DIR="$(mktemp -d /tmp/$(basename $0).XXXXXX)"
find $(pwd)/.. -name 'yandex-yt-local_*.deb' -exec cp -r {} $TMP_DIR \;
cd "$TMP_DIR"

dpkg -x yandex-yt-local_*.deb "yandex-yt-local"
download_and_extract yandex-yt-python $YANDEX_YT_PYTHON_VERSION
download_and_extract yandex-yt $YANDEX_YT_VERSION
download_and_extract yandex-yt-http-proxy $YANDEX_YT_VERSION
download_and_extract yandex-yt-python-yson $YANDEX_YT_YSON_BINDINGS_VERSION
download_and_extract yandex-yt-web-interface

mkdir -p "archive/bin"
mkdir -p "archive/python/yt"
mkdir -p "archive/yt-thor"

cp -r yandex-yt/usr/bin/ytserver archive/bin
cp -r yandex-yt-http-proxy/usr/lib/node_modules archive/
cp -r yandex-yt-python/usr/share/pyshared/yt/* archive/python/yt
cp -r yandex-yt-python/usr/bin/yt2 archive/bin/
cp -r yandex-yt-local/usr/share/pyshared/yt/* archive/python/yt
cp -r yandex-yt-local/usr/bin/yt_local archive/bin/
cp -r yandex-yt-python-yson/usr/share/pyshared/yt/* archive/python/yt
cp -r yandex-yt-web-interface/usr/share/yt-thor/* archive/yt-thor

archive_name="yt_local"
tar cvfz $ARCHIVE_NAME archive/
cat $ARCHIVE_NAME | yt2 upload //home/files/${ARCHIVE_NAME} --proxy locke

rm -rf "$TMP_DIR"
