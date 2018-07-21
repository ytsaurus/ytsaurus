#!/bin/bash -eux

./deploy_provider.sh yandex-yp-python

FILES="MANIFEST.in debian/changelog"
for f in $FILES; do
    cp "yandex-yp-python/$f" "yandex-yp-python-skynet/$f"
done

./deploy_provider.sh yandex-yp-python-skynet

for f in $FILES; do
    rm "yandex-yp-python-skynet/$f"
done
