#!/bin/bash

set -ex

apt-get update

apt-get install -y --force-yes \
    build-essential \
    checkinstall \
    libreadline-gplv2-dev \
    libncursesw5-dev \
    libssl-dev \
    libsqlite3-dev \
    tk-dev \
    libgdbm-dev \
    libc6-dev \
    libbz2-dev \
    libssl-dev \
    openssl \
    lzma \
    lzma-dev \
    liblzma-dev

wget https://www.python.org/ftp/python/3.4.4/Python-3.4.4.tgz
tar xzf Python-3.4.4.tgz
cd Python-3.4.4
./configure --prefix=/opt/python3.4 --with-zlib-dir=/usr/local/lib/ --with-ensurepip=install
make altinstall


cd ..
rm -rf Python-3.4.4
rm -rf Python-3.4.4.tgz
apt-get remove --purge -y --force-yes \
    build-essential \
    checkinstall \
    libreadline-gplv2-dev \
    libncursesw5-dev \
    libssl-dev \
    libsqlite3-dev \
    tk-dev \
    libgdbm-dev \
    libc6-dev \
    libbz2-dev \
    libssl-dev \
    openssl \
    lzma \
    lzma-dev \
    liblzma-dev

/opt/python3.4/bin/pip install -i https://pypi.yandex-team.ru/simple yandex-yt==0.9.29
