#!/bin/bash
set -e

machine=$(hostname -f)

case ${machine} in
    "build01-01g.yt.yandex.net")
        CELLAR=/home/sandello/Cellar/stage
        SOURCE=/home/sandello/source-clean
        TARGET=/home/sandello/doxygen
        ;;
    "CoreMoonlight.local")
        CELLAR=/usr/local/Cellar/doxygen/1.7.4/
        SOURCE=/Users/sandello/YT/source
        TARGET=/Users/sandello/YT/doxygen
        ;;
    *)
        echo "Unknown machine; please, include your configuration into common.sh."
        exit 1
esac

