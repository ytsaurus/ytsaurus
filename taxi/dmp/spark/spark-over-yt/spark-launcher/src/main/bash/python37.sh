#!/bin/bash

set -ex


cat <<EOT > /etc/apt/sources.list.d/yandex-taxi-xenial.list
deb http://yandex-taxi-xenial.dist.yandex.ru/yandex-taxi-xenial prestable/all/
deb http://yandex-taxi-xenial.dist.yandex.ru/yandex-taxi-xenial prestable/amd64/
deb http://yandex-taxi-xenial.dist.yandex.ru/yandex-taxi-xenial stable/all/
deb http://yandex-taxi-xenial.dist.yandex.ru/yandex-taxi-xenial stable/amd64/
deb http://yandex-taxi-xenial.dist.yandex.ru/yandex-taxi-xenial testing/all/
deb http://yandex-taxi-xenial.dist.yandex.ru/yandex-taxi-xenial testing/amd64/
deb http://yandex-taxi-xenial.dist.yandex.ru/yandex-taxi-xenial unstable/all/
deb http://yandex-taxi-xenial.dist.yandex.ru/yandex-taxi-xenial unstable/amd64/
EOT

apt-get update
apt-get install -y python3.7 python3-pip
python3.7 -m pip install -i https://pypi.yandex-team.ru/simple yandex-yt==0.9.29
python3.7 -m pip install -i https://pypi.yandex-team.ru/simple pyarrow==0.17.1
python3.7 -m pip install -i https://pypi.yandex-team.ru/simple pandas==0.24.2
python3.7 -m pip install -i https://pypi.yandex-team.ru/simple cyson
python3.7 -m pip install -i https://pypi.yandex-team.ru/simple scipy
python3.7 -m pip install -i https://pypi.yandex-team.ru/simple numpy==1.19.5

mkdir -p /opt/python3.7/bin
ln -s /usr/bin/python3.7 /opt/python3.7/bin/python
ln -s /usr/bin/python3.7 /opt/python3.7/bin/python3.7
