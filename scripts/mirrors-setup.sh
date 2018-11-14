#!/bin/bash

MIRRORS_DIRECTORY="$HOME/mirrors"

set -x
set -e

# git-svn may mangle commit messages when running with improper encoding.
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8

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
    path=${name//-//}
    path=${path/\/\//-}
    xname=arcadia-$name
    root=$MIRRORS_DIRECTORY/$xname.git

    create_stub_repository $root || true

    (cd $root && git config --local svn-remote.svn.url "svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia/$path")
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
    path=${name//-//}
    path=${path/\/\//-}
    xname=github-$name
    root=$MIRRORS_DIRECTORY/$xname.git

    create_stub_repository $root || true

    (cd $root && git config --remove-section remote.github) || true
    (cd $root && git config --local remote.github.url "git://github.com/$path")
    (cd $root && git config --local remote.github.mirror true)
    (cd $root && git config --local remote.github.tagopt --no-tags)
    (cd $root && git config --local --unset-all remote.github.fetch) || true
    (cd $root && git config --local --add remote.github.fetch "+HEAD:refs/upstream/HEAD")
    (cd $root && git config --local --add remote.github.fetch "+refs/heads/*:refs/upstream/heads/*")
    (cd $root && git config --local --add remote.github.fetch "+refs/tags/*:refs/upstream/tags/*")

    (cd $root && git config --remove-section remote.origin) || true
    (cd $root && git config --local remote.origin.url "git@github.yandex-team.ru:yt/$xname.git")
    (cd $root && git config --local remote.origin.mirror false)
    (cd $root && git config --local --unset-all remote.origin.push) || true
    (cd $root && git config --local --add remote.origin.push "+refs/upstream/HEAD:refs/heads/upstream/HEAD")
    (cd $root && git config --local --add remote.origin.push "+refs/upstream/heads/*:refs/heads/upstream/*")
    (cd $root && git config --local --add remote.origin.push "+refs/upstream/tags/*:refs/tags/upstream/*")
}

################################################################################

mkdir -p $MIRRORS_DIRECTORY

configure_arcadia_mirror contrib-libs-base64
configure_arcadia_mirror contrib-libs-c--ares
configure_arcadia_mirror contrib-libs-libbz2
configure_arcadia_mirror contrib-libs-libiconv
configure_arcadia_mirror contrib-libs-lz4
configure_arcadia_mirror contrib-libs-lzmasdk
configure_arcadia_mirror contrib-libs-minilzo
configure_arcadia_mirror contrib-libs-openssl
configure_arcadia_mirror contrib-libs-protobuf
configure_arcadia_mirror contrib-libs-re2
configure_arcadia_mirror contrib-libs-snappy
configure_arcadia_mirror contrib-libs-sparsehash
configure_arcadia_mirror contrib-libs-yajl
configure_arcadia_mirror contrib-libs-zlib
configure_arcadia_mirror contrib-libs-zstd
configure_arcadia_mirror contrib-libs-gtest
configure_arcadia_mirror contrib-libs-gmock
configure_arcadia_mirror contrib-libs-grpc
configure_arcadia_mirror contrib-libs-nanopb

configure_arcadia_mirror library-getopt
configure_arcadia_mirror library-http
configure_arcadia_mirror library-lfalloc
configure_arcadia_mirror library-malloc-api
configure_arcadia_mirror library-openssl
configure_arcadia_mirror library-streams-lz
configure_arcadia_mirror library-streams-lzop
configure_arcadia_mirror library-string_utils-base64
configure_arcadia_mirror library-threading-future

configure_arcadia_mirror mapreduce-yt-interface-protos

configure_arcadia_mirror util

configure_github_mirror lloyd-yajl
configure_github_mirror joyent-libuv
configure_github_mirror twitter-zipkin
configure_github_mirror pyinstaller-pyinstaller
configure_github_mirror google-benchmark
configure_github_mirror kennethreitz-requests
configure_github_mirror google-brotli
configure_github_mirror uqfoundation-dill
configure_github_mirror kislyuk-argcomplete
configure_github_mirror pympler-pympler
configure_github_mirror cherrypy-cherrypy
configure_github_mirror tornadoweb-tornado
configure_github_mirror certifi-python-certifi
configure_github_mirror cython-backports_abc
configure_github_mirror micheles-decorator
configure_github_mirror dateutil-dateutil
configure_github_mirror terencehonles-fusepy
configure_github_mirror tqdm-tqdm
configure_github_mirror kxxoling-PTable
configure_github_mirror nir0s-distro
configure_github_mirror urllib3-urllib3
