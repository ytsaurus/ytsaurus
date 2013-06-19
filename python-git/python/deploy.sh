#!/bin/bash -eux


#make deb_without_test

VERSION=$(dpkg-parsechangelog | grep Version | awk '{print $2}')
dupload ../yandex-yt-python_${VERSION}_amd64.changes

