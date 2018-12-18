#!/bin/bash -ex

set -o pipefail

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

    local current_dir="$(pwd)"

    local tmp_dir="$(mktemp --tmpdir -d $(basename $0).XXXXXX)"
    local archive_dir="$(mktemp --tmpdir -d yt_local_archive.XXXXXX)"
    trap "rm -rf \"$tmp_dir\" \"$archive_dir\"" EXIT

    local yt_local_version=$(dpkg-parsechangelog | grep Version | awk '{print $2}')

    local local_package_dir="$(pwd)/.."
    local local_package_pattern='yandex-yt-local_'"${yt_local_version}"'_*.deb'
    cd "$tmp_dir"
    if [ $(find "$local_package_dir" -name "$local_package_pattern" | wc -l) -gt 0 ]; then
        find "$local_package_dir" -name "$local_package_pattern" -exec cp -r {} $tmp_dir \;
        dpkg -x yandex-yt-local_*.deb "yandex-yt-local"
    else
        download_and_extract yandex-yt-local $yt_local_version
    fi
    download_and_extract yandex-yt-node $yt_version
    download_and_extract yandex-yt-proxy $yt_version
    download_and_extract yandex-yt-master $yt_version
    download_and_extract yandex-yt-scheduler $yt_version
    download_and_extract yandex-yt-controller-agent $yt_version
    download_and_extract yandex-yt-http-proxy $yt_version
    download_and_extract yandex-yt-python-driver $yt_version
    download_and_extract yandex-yt-python $yt_python_version
    download_and_extract yandex-yt-python-yson $yt_yson_bindings_version

    mkdir -p "$archive_dir/bin"
    mkdir -p "$archive_dir/python/yt"
    mkdir -p "$archive_dir/python/yt_yson_bindings"
    mkdir -p "$archive_dir/python/yt_driver_bindings"

    for dir in yandex-yt-master yandex-yt-scheduler yandex-yt-proxy yandex-yt-node yandex-yt-controller-agent; do
        for binary in $(find $dir/usr/bin -name "ytserver*"); do
            cp -r "$binary" "$archive_dir/bin"
        done
    done

    cp -r yandex-yt-python/usr/share/pyshared/yt/* "$archive_dir/python/yt"
    cp -r yandex-yt-python/usr/bin/yt2 "$archive_dir/bin"
    cp -r yandex-yt-python/usr/bin/mapreduce-yt2 "$archive_dir/bin"

    local current_path="$(pwd)"
    cd "$archive_dir/bin"
    ln -s mapreduce-yt2 mapreduce-yt
    cd "$current_path"

    cp -r yandex-yt-local/usr/share/pyshared/yt/* "$archive_dir/python/yt"
    cp -r yandex-yt-local/usr/bin/yt_local "$archive_dir/bin"
    cp -r yandex-yt-local/usr/bin/yt_env_watcher "$archive_dir/bin"

    # YSON bindings.
    cp -r -L yandex-yt-python-yson/usr/lib/python2.7/dist-packages/yt_yson_bindings/* "$archive_dir/python/yt_yson_bindings"
    # Driver bindings.
    if [[ -d "yandex-yt-python-driver/usr/lib/python2.7/dist-packages/yt_driver_bindings" ]]; then
        # New packaging system directory layout
        cp -r -L yandex-yt-python-driver/usr/lib/python2.7/dist-packages/yt_driver_bindings/* "$archive_dir/python/yt_driver_bindings"
    else
        cp -r -L yandex-yt-python-driver/usr/share/pyshared/yt_driver_bindings/* "$archive_dir/python/yt_driver_bindings"
        cp -r -L yandex-yt-python-driver/usr/lib/pyshared/python2.7/yt_driver_bindings/* "$archive_dir/python/yt_driver_bindings"
    fi

    cd "$current_dir"

    rm -rf "$tmp_dir"

    echo "$archive_dir"
}

if [ "$(find $(pwd)/.. -name 'yandex-yt-local_*.deb' | wc -l)" = "0" ]; then
    # Package missing, let's build it.
    DEB_STRIP_EXCLUDE=".*" DEB=1 dpkg-buildpackage -i -I -rfakeroot
fi

prepare_archive_directory "$1" "$2" "$3" | tee "yt_local_archive_path"
