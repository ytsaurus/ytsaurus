#!/bin/bash

# Fixes shebangs after distutils.
for bin in $(find "debian/${PYBUILD_NAME}/usr/bin" -mindepth 1); do
    if $(head -n 1 "$bin" | grep -q '^#!.*python[0-9.]*'); then
        echo "Fixing shebang back to /usr/bin/python in $bin"
        sed -i '1 s/^.*$/#!\/usr\/bin\/python/g' "$bin"
    fi
done
