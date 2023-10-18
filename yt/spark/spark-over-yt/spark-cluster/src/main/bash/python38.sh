#!/bin/bash

set -ex

# apt-update.sh
ln -sf /dev/null /etc/systemd/system/apt-daily-upgrade.service
ln -sf /dev/null /etc/systemd/system/apt-daily.service
if [ -e "/bin/systemctl" ]; then
    # wait until systemd is completely loaded
    while true; do
        sleep 2
        STATUS="$(/bin/systemctl status | awk '$1 == "State:" {print $2}')"
        [ "${STATUS}" = "running" ] && break
        [ "${STATUS}" = "degraded" ] && break
    done
fi
apt-get update

PYTHON_VERSION="python3.8"

apt-get install -y software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update --allow-unauthenticated
apt-get install -y build-essential manpages-dev libpcap-dev libpq-dev libssl-dev libffi-dev
apt-get install -y $PYTHON_VERSION $PYTHON_VERSION-dev $PYTHON_VERSION-distutils $PYTHON_VERSION-venv
curl -sS https://bootstrap.pypa.io/get-pip.py | $PYTHON_VERSION
$PYTHON_VERSION -m pip install --upgrade pip
$PYTHON_VERSION -m pip uninstall yandex-yt
$PYTHON_VERSION -m pip install -i https://pypi.yandex-team.ru/simple ytsaurus-client==0.13.7
$PYTHON_VERSION -m pip install -i https://pypi.yandex-team.ru/simple pyarrow==12.0.1
$PYTHON_VERSION -m pip install -i https://pypi.yandex-team.ru/simple pandas==1.5.3
# $PYTHON_VERSION -m pip install -i https://pypi.yandex-team.ru/simple cython==3.0.0b1
# $PYTHON_VERSION -m pip install -i https://pypi.yandex-team.ru/simple cyson
$PYTHON_VERSION -m pip install -i https://pypi.yandex-team.ru/simple scipy==1.10.1
$PYTHON_VERSION -m pip install -i https://pypi.yandex-team.ru/simple numpy==1.24.2

mkdir -p /opt/$PYTHON_VERSION/bin
ln -s /usr/bin/$PYTHON_VERSION /opt/$PYTHON_VERSION/bin/python
ln -s /usr/bin/$PYTHON_VERSION /opt/$PYTHON_VERSION/bin/$PYTHON_VERSION
