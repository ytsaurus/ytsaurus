#!/bin/bash -ex
# Upload archive with all necessary stuff to Locke.

export YT_PROXY=locke.yt.yandex.net
export YT_TOKEN=1da6afc98d189e8ba59d2ea39f29d0f1  # Teamcity user
export PYTHONPATH="$(pwd)"
YT="$(pwd)/yt/wrapper/yt"

YANDEX_YT_LOCAL_VERSION=$(dpkg-parsechangelog | grep Version | awk '{print $2}')
YANDEX_YT_PYTHON_VERSION="0.6.67-0"
YANDEX_YT_VERSION="0.17.4-stable-without-yt~6595~df8d03c"
YANDEX_YT_YSON_BINDINGS_VERSION="0.2.23-0"
NODEJS_VERSION="0.8.26"

UBUNTU_CODENAME=$(lsb_release -c -s)

VERSIONS="${YANDEX_YT_LOCAL_VERSION}${YANDEX_YT_PYTHON_VERSION}${YANDEX_YT_VERSION}${YANDEX_YT_YSON_BINDINGS_VERSION}"
HASH=$(echo -ne $VERSIONS | md5sum | head -c 8)
ARCHIVE_NAME="yt_local_${HASH}_${UBUNTU_CODENAME}_archive.tgz"

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
download_and_extract yandex-yt-python-driver $YANDEX_YT_VERSION
download_and_extract yandex-yt-python-yson $YANDEX_YT_YSON_BINDINGS_VERSION
download_and_extract yandex-yt-web-interface
download_and_extract nodejs $NODEJS_VERSION

mkdir -p "archive/bin"
mkdir -p "archive/python/yt"
mkdir -p "archive/python/yt_yson_bindings"
mkdir -p "archive/python/yt_driver_bindings"
mkdir -p "archive/yt-thor"
mkdir -p "archive/node"

cp -r yandex-yt/usr/bin/ytserver archive/bin
cp -r yandex-yt-http-proxy/usr/lib/node_modules archive/

cp -r yandex-yt-python/usr/share/pyshared/yt/* archive/python/yt
cp -r yandex-yt-python/usr/bin/yt2 archive/bin/
cp -r yandex-yt-python/usr/bin/mapreduce-yt archive/bin

cp -r yandex-yt-local/usr/share/pyshared/yt/* archive/python/yt
cp -r yandex-yt-local/usr/bin/yt_local archive/bin/

# YSON bindings.
cp -r yandex-yt-python-yson/usr/share/pyshared/yt/* archive/python/yt
cp -r yandex-yt-python-yson/usr/share/pyshared/yt_yson_bindings/* archive/python/yt_yson_bindings
cp -r yandex-yt-python-yson/usr/lib/pyshared/python2.7/yt/* archive/python/yt
cp -r yandex-yt-python-yson/usr/lib/pyshared/python2.7/yt_yson_bindings/* archive/python/yt_yson_bindings

# Driver bindings.
cp -r yandex-yt-python-driver/usr/share/pyshared/yt_driver_bindings/* archive/python/yt_driver_bindings
cp -r yandex-yt-python-driver/usr/lib/pyshared/python2.7/yt_driver_bindings/* archive/python/yt_driver_bindings

cp -r yandex-yt-web-interface/usr/share/yt-thor/* archive/yt-thor

cp -r nodejs/usr/* archive/node

tar cvfz $ARCHIVE_NAME archive/
cat $ARCHIVE_NAME | $YT upload //home/files/${ARCHIVE_NAME} --proxy locke
$YT set //home/files/${ARCHIVE_NAME}/@packages_versions "{\
      yandex-yt=\"$YANDEX_YT_VERSION\"; \
      yandex-yt-local=\"$YANDEX_YT_LOCAL_VERSION\";\
      yandex-yt-python=\"$YANDEX_YT_PYTHON_VERSION\";\
      yandex-yt-python-yson=\"$YANDEX_YT_YSON_BINDINGS_VERSION\"}" --proxy locke

rm -rf "$TMP_DIR"
