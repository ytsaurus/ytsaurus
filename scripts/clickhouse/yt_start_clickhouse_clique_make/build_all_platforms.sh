#!/bin/bash

for platform in linux darwin windows ; do
    echo "Building for platform $platform"
    mkdir -p ../../../../../build-rel/bin/$platform
    ya make -I ../../../../../build-rel/bin/$platform -r --target-platform ${platform}
done
