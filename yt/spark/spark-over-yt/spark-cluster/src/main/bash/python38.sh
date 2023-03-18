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

apt-get install -y software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update --allow-unauthenticated
apt-get install -y build-essential manpages-dev libpcap-dev libpq-dev libssl-dev libffi-dev
apt-get install -y python3.8 python3.8-dev python3.8-distutils python3.8-venv
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.8
python3.8 -m pip install --upgrade pip
python3.8 -m pip install -i https://pypi.yandex-team.ru/simple yandex-yt==0.9.29
python3.8 -m pip install -i https://pypi.yandex-team.ru/simple pyarrow==11.0.0
python3.8 -m pip install -i https://pypi.yandex-team.ru/simple pandas==1.5.3
# python3.8 -m pip install -i https://pypi.yandex-team.ru/simple cython==3.0.0b1
# python3.8 -m pip install -i https://pypi.yandex-team.ru/simple cyson
python3.8 -m pip install -i https://pypi.yandex-team.ru/simple scipy==1.10.1
python3.8 -m pip install -i https://pypi.yandex-team.ru/simple numpy==1.24.2

mkdir -p /opt/python3.8/bin
ln -s /usr/bin/python3.8 /opt/python3.8/bin/python
ln -s /usr/bin/python3.8 /opt/python3.8/bin/python3.8
