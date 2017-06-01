#!/bin/bash


usage() {
    echo "Usage: copy_packages.sh <action> <params...>"
    echo
    echo "Actions:"
    echo "    download <repository> <version>  - download .deb packages, where <repository> is yt-precise or yt-trusty"
    echo "    upload <repository> <version>    - upload .deb packages, where <repository> is yabs-precise or yabs-trusty"
    echo "    clean                            - clean current dirctory"
    echo
    exit 1
}

clean()
{
    rm *.deb
    rm *.changes
    rm -rf debian
}

CONTROL='''Source: yandex-yt
Section: unknown
Priority: extra
Maintainer: YT Team <yt@yandex-team.ru>
Standards-Version: 3.9.1
Homepage: http://wiki.yandex-team.ru/YT
'''

download() {
    local repository=$1
    local verison=$2

    local urls="$(curl -s http://dist.yandex.ru/$repository/unstable/amd64/Packages.bz2 | bzip2 -d -c | fgrep $version | fgrep Filename | cut -d' ' -f 2)"

    mkdir -p $version
    cd $version
    mkdir -p debian

    echo "$CONTROL" > debian/control

    for url in $urls; do
        local name="$(echo $url | rev | cut -d/ -f 1 | cut -d. -f 1 --complement | rev)"
        local package="$(echo $name | cut -d_ -f 1)"
        local suffix="$(echo $name | cut -d_ -f 2)"

        echo "Downloading package: ${package}, url: ${url}"
        curl -s "http://dist.yandex.ru/${url}" > "${name}.deb"

        if [ "$package" == "yandex-yt" ]; then
            dpkg --fsys-tarfile "${name}.deb" | tar xOf - ./usr/share/doc/yandex-yt/changelog.Debian.gz | zcat > debian/changelog
        fi

        echo "" >> debian/control        
        dpkg-deb -f "${name}.deb" Package Architecture Depends Description Conflicts >> debian/control

        dpkg-distaddfile "${name}.deb" unknown extra
    done

    local changes_file="yandex-yt_${version}.changes"

    echo "Generating changes..."
    dpkg-genchanges -b -u. > $changes_file

    echo "Generating signature..."
    gpg --utf8-strings --clearsign --armor --textmode $changes_file
    mv "${changes_file}.asc" $changes_file
}

upload() {
    local repository=$1
    local verison=$2

    cd $version

    local changes_file="yandex-yt_${version}.changes"

    dupload --to $repository --nomail --force $changes_file
}

while [ $# -gt 0 ]; do
  case $1 in
    download)
      repository=$2
      version=$3

      if [ -z "${repository}" -o -z "${version}" ]; then
          echo "Error: Repository or version is not defined"
          echo
          usage
      fi
      download $repository $version

      shift; shift; shift
      ;;
    upload)
      repository=$2
      version=$3

      if [ -z "${repository}" -o -z "${version}" ]; then
          echo "Error: Repository or version is not defined"
          echo
          usage
      fi

      upload $repository $version

      shift; shift; shift
      ;;
    clean)
      clean
      shift
      ;;
   
    --help)
      usage
      shift
    ;;
    *)
      action=$1
      echo "Error: Unknown action ${action}."
      echo
      usage

      shift
    ;;
  esac
done

