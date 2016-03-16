#!/bin/bash -ex
# Upload archive with all necessary stuff to Locke.

export YT_PROXY=locke.yt.yandex.net
export YT_TOKEN=1da6afc98d189e8ba59d2ea39f29d0f1  # Teamcity user
export PYTHONPATH="$(pwd)"
YT="$(pwd)/yt/wrapper/yt"

UBUNTU_CODENAME=$(lsb_release -c -s)

if [ "$(find $(pwd)/.. -name 'yandex-yt-local_*.deb' | wc -l)" = "0" ]; then
    # Package missing, let's build it.
    DEB=1 python setup.py sdist --dist-dir=../
    DEB_STRIP_EXCLUDE=".*" DEB=1 dpkg-buildpackage -i -I -rfakeroot
fi

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

create_archive() {
    local yt_local_version="$1" && shift
    local yt_version="$1" && shift
    local yt_python_version="$1" && shift
    local yt_yson_bindings_version="$1" && shift

    local nodejs_version="0.8.26"  # Fixed for now

    echo -ne "Making archive with the following packages:\n" \
             "    yandex-yt=${yt_version}\n" \
             "    yandex-yt-local=${yt_local_version}\n" \
             "    yandex-yt-python=${yt_python_version}\n" \
             "    yandex-yt-python-yson=${yt_yson_bindings_version}\n"

    local current_dir="$(pwd)"

    local versions_str="${yt_local_version}${yt_python_version}${yt_version}${yt_yson_bindings_version}"
    local hash_str=$(echo -ne $versions_str | md5sum | head -c 8)
    local archive_name="yt_local_${hash_str}_${UBUNTU_CODENAME}_archive.tgz"

    local archive_path="//home/files/${archive_name}"

    if [ "$($YT exists $archive_path --proxy locke)" = "true" ]; then
        echo "Appropriate archive already exists"
        return
    fi

    local tmp_dir="$(mktemp -d /tmp/$(basename $0).XXXXXX)"
    find $(pwd)/.. -name 'yandex-yt-local_*.deb' -exec cp -r {} $tmp_dir \;
    cd "$tmp_dir"

    dpkg -x yandex-yt-local_*.deb "yandex-yt-local"
    download_and_extract yandex-yt $yt_version
    download_and_extract yandex-yt-http-proxy $yt_version
    download_and_extract yandex-yt-python-driver $yt_version
    download_and_extract yandex-yt-python $yt_python_version
    download_and_extract yandex-yt-python-yson $yt_yson_bindings_version
    download_and_extract yandex-yt-web-interface
    download_and_extract nodejs $nodejs_version

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
    cp -r yandex-yt-python-yson/usr/share/pyshared/yt_yson_bindings/* archive/python/yt_yson_bindings
    cp -r yandex-yt-python-yson/usr/lib/pyshared/python2.7/yt_yson_bindings/* archive/python/yt_yson_bindings

    # Driver bindings.
    cp -r yandex-yt-python-driver/usr/share/pyshared/yt_driver_bindings/* archive/python/yt_driver_bindings
    cp -r yandex-yt-python-driver/usr/lib/pyshared/python2.7/yt_driver_bindings/* archive/python/yt_driver_bindings

    cp -r yandex-yt-web-interface/usr/share/yt-thor/* archive/yt-thor

    cp -r nodejs/usr/* archive/node

    tar cvfz "$archive_name" -C "archive" .

    cat "$archive_name" | $YT upload "$archive_path" --proxy locke
    $YT set --proxy locke //home/files/${archive_name}/@packages_versions "{\
          yandex-yt=\"$yt_version\"; \
          yandex-yt-local=\"$yt_local_version\";\
          yandex-yt-python=\"$yt_python_version\";\
          yandex-yt-python-yson=\"$yt_yson_bindings_version\"}"

    cd "$current_dir"

    rm -rf "$tmp_dir"

    echo "Done! Archive path: $archive_path"
}

YANDEX_YT_LOCAL_VERSION=$(dpkg-parsechangelog | grep Version | awk '{print $2}')
YANDEX_YT_PYTHON_VERSION="0.6.89-0"
YANDEX_YT_VERSIONS="0.17.5-prestable-without-yt~7966~df46c24 18.2.19636-prestable-without-yt~ba0b505"
YANDEX_YT_PYTHON_BINDINGS_VERSION="0.2.26-0"

for YANDEX_YT_VERSION in $YANDEX_YT_VERSIONS; do
    create_archive "$YANDEX_YT_LOCAL_VERSION" \
                   "$YANDEX_YT_VERSION" \
                   "$YANDEX_YT_PYTHON_VERSION" \
                   "$YANDEX_YT_PYTHON_BINDINGS_VERSION"
done
