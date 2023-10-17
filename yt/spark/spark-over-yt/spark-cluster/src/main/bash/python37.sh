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
apt-get install -y python3.7 python3.7-dev python3.7-distutils python3.7-venv
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.7
python3.7 -m pip install --upgrade pip
python3.7 -m pip install -i https://pypi.yandex-team.ru/simple numpy==1.21.6
python3.7 -m pip install -i https://pypi.yandex-team.ru/simple ytsaurus-client==0.13.7
python3.7 -m pip install -i https://pypi.yandex-team.ru/simple pyarrow==12.0.1
python3.7 -m pip install -i https://pypi.yandex-team.ru/simple pandas==1.1.5
python3.7 -m pip install -i https://pypi.yandex-team.ru/simple cyson
python3.7 -m pip install -i https://pypi.yandex-team.ru/simple scipy==1.7.3
python3.7 -m pip install -i https://pypi.yandex-team.ru/simple pyyaml==6.0

mkdir -p /opt/python3.7/bin
ln -s /usr/bin/python3.7 /opt/python3.7/bin/python
ln -s /usr/bin/python3.7 /opt/python3.7/bin/python3.7
