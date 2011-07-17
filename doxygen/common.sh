#!/bin/bash
set -e

machine=$(hostname -f)

case ${machine} in
    "yt-dev01d.yandex-net")
        CELLAR=/home/sandello/Cellar/stage
        SOURCE=/home/sandello/source
        ;;
    "CoreMoonlight.local")
        CELLAR=/usr/local/Cellar/doxygen/1.7.4/
        SOURCE=/Users/sandello/YT/source
        ;;
    *)
        echo "Unknown machine; please, include your configuration into common.sh."
        exit 1
esac

