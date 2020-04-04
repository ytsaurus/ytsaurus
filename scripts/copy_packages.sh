#!/bin/bash

usage() {
    echo "Usage: copy_packages.sh <version> <source repository> <destination repository>"
    echo
    echo "Source repository: yt-precise or yt-trusty"
    echo "Destination repository: yabs-precise or yabs-trusty"
    echo
    exit 1
}

CONTROL='''Source: yandex-yt
Section: unknown
Priority: extra
Maintainer: YT Team <yt@yandex-team.ru>
Standards-Version: 3.9.1
Homepage: http://wiki.yandex-team.ru/YT
'''

get_changes_filename()
{
    local verison=$1
    echo "yandex-yt_${version}_amd64.changes"
}

download() {
    local repository=$1
    local verison=$2

    local urls="$(curl -s http://dist.yandex.ru/$repository/unstable/amd64/Packages.bz2 | bzip2 -d -c | fgrep $version | fgrep Filename | cut -d' ' -f 2)"

    cd $version
    mkdir -p debian

    echo "$CONTROL" > debian/control

    for url in $urls; do
        local name="$(echo $url | rev | cut -d/ -f 1 | cut -d. -f 1 --complement | rev)"
        local package="$(echo $name | cut -d_ -f 1)"
        local suffix="$(echo $name | cut -d_ -f 2)"

        echo "Downloading package: ${package}, url: ${url}"
        curl -s "http://dist.yandex.ru/${url}" > "${name}.deb"

        if [ "$package" == "yandex-yt-node" ]; then
            dpkg --fsys-tarfile "${name}.deb" | tar xOf - ./usr/share/doc/yandex-yt-node/changelog.Debian.gz | zcat > debian/changelog
	    sed -i 's/yandex-yt-node/yandex-yt/g' debian/changelog
        fi

        echo "" >> debian/control
        dpkg-deb -f "${name}.deb" Package Architecture Depends Description Conflicts >> debian/control

        dpkg-distaddfile "${name}.deb" unknown extra
    done

    local changes_file=$(get_changes_filename $version) #"yandex-yt_${version}_amd64.changes"

    echo "Generating changes..."
    dpkg-genchanges -b -u. > $changes_file

    echo "Generating signature..."
    gpg --utf8-strings --clearsign --armor --textmode -u $USER $changes_file
    mv "${changes_file}.asc" $changes_file
    cd ..
}


upload() {
    local repository=$1
    local verison=$2

    cd $version

    local changes_file=$(get_changes_filename $version) #"yandex-yt_${version}.changes"

    dupload --to $repository --nomail --force $changes_file
    cd ..
}

if [ "$1" == "--help" ]; then
  usage
fi

version=$1
src_repository=$2
dst_repository=$3

if [ -z "$version" ]; then
  echo "Error: Version is not defined"
  echo
  usage
fi

if [ -z "$src_repository" ]; then
  echo "Error: Source repository is not defined"
  echo
  usage
fi

if [ -z "$dst_repository" ]; then
  echo "Error: Destination repository is not defined"
  echo
  usage
fi

mkdir -p $version
download $src_repository $version
upload $dst_repository $version
rm -rf $version
