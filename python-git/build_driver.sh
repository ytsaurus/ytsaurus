#!/bin/bash

FORCE_BUILD=1 SKIP_WHEEL=1 ./deploy.sh yandex-yt-python-driver
FORCE_BUILD=1 ./deploy.sh yandex-yt-python-driver-rpc
