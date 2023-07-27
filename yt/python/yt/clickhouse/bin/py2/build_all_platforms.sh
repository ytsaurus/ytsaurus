#!/bin/bash

if [ "$1" == "" ] ; then
    echo "Specify exactly one command-line argument with destination directory where to put three binaries" >&2
    exit 1
fi

for platform in linux darwin windows ; do
    echo "Building for platform $platform"
    mkdir -p $1/$platform
    ya make -I $1/$platform -r --target-platform ${platform} -DFORCE_VCS_INFO_UPDATE=yes
done
