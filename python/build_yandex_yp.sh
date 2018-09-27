#!/bin/bash -eux

./deploy_provider.sh yandex-yp-python

FILES="MANIFEST.in debian/changelog"
for f in $FILES; do
    rm -f "yandex-yp-python-skynet/$f"
    if [ "$f" = "debian/changelog" ]; then
        replace_script="import sys; sys.stdout.write(sys.stdin.read().replace('yandex-yp-python', 'yandex-yp-python-skynet'))"
        python -c "$replace_script" <"yandex-yp-python/$f" >"yandex-yp-python-skynet/$f"
    else
        cp "yandex-yp-python/$f" "yandex-yp-python-skynet/$f"
    fi
done

./deploy_provider.sh yandex-yp-python-skynet

for f in $FILES; do
    rm "yandex-yp-python-skynet/$f"
done
