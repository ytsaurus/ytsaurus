#!/bin/bash -ex

download_and_extract() {
    local package="$1"
    if [ -n "$2" ]; then
        version_suffix="=$2"
    else
        version_suffix=""
    fi
    apt-get download "$package$version_suffix" -qq 1>&2
    dpkg -x ${package}_*.deb $package 1>&2
}

prepare_archive_directory() {
    local yt_version="$1" && shift
    local yt_python_version="$1" && shift
    local yt_yson_bindings_version="$1" && shift

    local nodejs_version="0.8.26"  # Fixed for now

    local current_dir="$(pwd)"

    local tmp_dir="$(mktemp -d /tmp/$(basename $0).XXXXXX)"
    local archive_dir="$(mktemp -d /tmp/yt_local_archive.XXXXXX)"

    find "$(pwd)/.." -name 'yandex-yt-local_*.deb' -exec cp -r {} $tmp_dir \;
    cd "$tmp_dir"

    dpkg -x yandex-yt-local_*.deb "yandex-yt-local"
    download_and_extract yandex-yt $yt_version
    download_and_extract yandex-yt-http-proxy $yt_version
    download_and_extract yandex-yt-python-driver $yt_version
    download_and_extract yandex-yt-python $yt_python_version
    download_and_extract yandex-yt-python-yson $yt_yson_bindings_version
    download_and_extract yandex-yt-web-interface
    download_and_extract nodejs $nodejs_version

    mkdir -p "$archive_dir/bin"
    mkdir -p "$archive_dir/python/yt"
    mkdir -p "$archive_dir/python/yt_yson_bindings"
    mkdir -p "$archive_dir/python/yt_driver_bindings"
    mkdir -p "$archive_dir/yt-thor"
    mkdir -p "$archive_dir/node"

    cp -r yandex-yt/usr/bin/ytserver "$archive_dir/bin"
    cp -r yandex-yt-http-proxy/usr/lib/node_modules "$archive_dir"

    cp -r yandex-yt-python/usr/share/pyshared/yt/* "$archive_dir/python/yt"
    cp -r yandex-yt-python/usr/bin/yt2 "$archive_dir/bin"
    cp -r yandex-yt-python/usr/bin/mapreduce-yt "$archive_dir/bin"

    cp -r yandex-yt-local/usr/share/pyshared/yt/* "$archive_dir/python/yt"
    cp -r yandex-yt-local/usr/bin/yt_local "$archive_dir/bin"

    # YSON bindings.
    cp -r yandex-yt-python-yson/usr/share/pyshared/yt_yson_bindings/* "$archive_dir/python/yt_yson_bindings"
    cp -r yandex-yt-python-yson/usr/lib/pyshared/python2.7/yt_yson_bindings/* "$archive_dir/python/yt_yson_bindings"

    # Driver bindings.
    cp -r yandex-yt-python-driver/usr/share/pyshared/yt_driver_bindings/* "$archive_dir/python/yt_driver_bindings"
    cp -r yandex-yt-python-driver/usr/lib/pyshared/python2.7/yt_driver_bindings/* "$archive_dir/python/yt_driver_bindings"

    cp -r yandex-yt-web-interface/usr/share/yt-thor/* "$archive_dir/yt-thor"

    cp -r nodejs/usr/* "$archive_dir/node"

    cd "$current_dir"

    rm -rf "$tmp_dir"

    echo "$archive_dir"
}

if [ "$(find $(pwd)/.. -name 'yandex-yt-local_*.deb' | wc -l)" = "0" ]; then
    # Package missing, let's build it.
    DEB=1 python setup.py sdist --dist-dir=../
    DEB_STRIP_EXCLUDE=".*" DEB=1 dpkg-buildpackage -i -I -rfakeroot
fi

prepare_archive_directory "$1" "$2" "$3" | tee "yt_local_archive_path"
