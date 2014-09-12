#!/bin/bash

MIRRORS_DIRECTORY="$HOME/mirrors"

set -x
set -e

################################################################################

create_stub_repository()
{
    root=$1

    mkdir $root
    mkdir $root/info
    mkdir $root/info/exclude
    mkdir $root/objects
    mkdir $root/objects/info
    mkdir $root/objects/pack
    mkdir $root/refs
    mkdir $root/refs/heads

    touch $root/config
    touch $root/HEAD

    echo 'ref: refs/heads/master' > $root/HEAD
    (cd $root && git config --local core.repositoryformatversion 0)
    (cd $root && git config --local core.filemode true)
    (cd $root && git config --local core.bare true)
    (cd $root && git config --local core.logallrefupdates false)
}

################################################################################

configure_arcadia_mirror()
{
    name=$1
    xname=arcadia-$name
    root=$MIRRORS_DIRECTORY/$xname.git

    create_stub_repository $root || true

    (cd $root && git config --local svn-remote.svn.url "svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia/${name//-//}")
    (cd $root && git config --local svn-remote.svn.fetch ":refs/remotes/git-svn")

    (cd $root && git config --remove-section remote.origin) || true
    (cd $root && git config --local remote.origin.url "git@github.yandex-team.ru:yt/$xname.git")
    (cd $root && git config --local remote.origin.push "+refs/remotes/git-svn:refs/heads/upstream")
    (cd $root && git config --local remote.origin.mirror false)
}

################################################################################

configure_github_mirror()
{
    name=$1
    xname=github-$name
    root=$MIRRORS_DIRECTORY/$xname.git

    create_stub_repository $root || true

    (cd $root && git config --remove-section remote.github) || true
    (cd $root && git config --local remote.github.url "git://github.com/${name//-//}")
    (cd $root && git config --local remote.github.mirror true)
    (cd $root && git config --local remote.github.tagopt --no-tags)
    (cd $root && git config --local --add remote.github.fetch "+refs/heads/*:refs/upstream/heads/*")
    (cd $root && git config --local --add remote.github.fetch "+refs/tags/*:refs/upstream/tags/*")

    (cd $root && git config --remove-section remote.origin) || true
    (cd $root && git config --local remote.origin.url "git@github.yandex-team.ru:yt/$xname.git")
    (cd $root && git config --local remote.origin.mirror false)
    (cd $root && git config --local --add remote.origin.push "+refs/upstream/*:refs/upstream/*")
    (cd $root && git config --local --add remote.origin.push "+refs/upstream/heads/master:refs/heads/upstream")
}

################################################################################

configure_arcadia_mirror contrib-libs-libiconv
configure_arcadia_mirror contrib-libs-protobuf
configure_arcadia_mirror contrib-libs-snappy
configure_arcadia_mirror contrib-libs-sparsehash
configure_arcadia_mirror contrib-libs-zlib

configure_arcadia_mirror util

configure_arcadia_mirror library-blockcodecs
configure_arcadia_mirror library-httpserver
configure_arcadia_mirror library-lfalloc
configure_arcadia_mirror library-lwtrace
configure_arcadia_mirror library-messagebus

configure_github_mirror lloyd-yajl
configure_github_mirror joyent-libuv
configure_github_mirror twitter-zipkin
configure_github_mirror pyinstaller-pyinstaller
