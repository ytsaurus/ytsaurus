#!/bin/bash

set -x

exit() {
    true
}

versions=(yandex-jdk8,8.181-tzdata2018f,jdk_8.tar.gz yandex-jdk10,10.0.2-tzdata2018f,jdk_10.tar.gz yandex-openjdk11,11.0.1-tzdata2018f,jdk_11.tar.gz)
clusters=(hahn arnold)

for i in ${versions[*]}
do
    IFS=","
    set $i
    jdk=$1    
    version=$2
    tar=$3
    deb="${jdk}_${version}_amd64.deb"
    dir="${jdk}_${version}"
    cdir="${dir}_control"
    echo "downloading ${jdk}=${version} to ${deb}"
    if [ ! -f $deb ]
    then
        apt-get download $jdk=$version
    fi
    echo "extracting $deb to $dir"
    rm -rf $dir $cdir $tar
    mkdir -p $dir   
    mkdir -p $cdir
    dpkg -x $deb $dir
    dpkg -e $deb $cdir 
    . $cdir/postinst abort-upgrade
    cwd=`pwd`    
    cd $dir
    mkdir -p usr/bin
    for CMD in $(ls ${_JDK_INSTALL_DIR}/bin); do
        if [ -f "${_JDK_INSTALL_DIR}/bin/${CMD}" ]
        then
            ln -sf "${_JDK_INSTALL_DIR}/bin/${CMD}" usr/bin/${CMD} 
        fi
    done
    tar czvf $cwd/$tar *
    cd ..
    echo "uploading $tar"
    IFS=" "
    for cluster in ${clusters[*]}
    do
        yt --proxy $cluster remove -f //porto_layers/${tar}.tmp
        cat $tar | yt --proxy $cluster upload //porto_layers/${tar}.tmp && yt --proxy $cluster move -f //porto_layers/${tar}.tmp //porto_layers/${tar}
        yt --proxy $cluster remove -f //porto_layers/${tar}.tmp
    done
done

