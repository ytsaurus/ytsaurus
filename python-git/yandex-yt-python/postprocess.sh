#!/bin/bash -eux
# Build and upload self-contained binaries

export YT_PROXY=locke.yt.yandex.net
export YT_TOKEN=1da6afc98d189e8ba59d2ea39f29d0f1 #teamcity user
export PYTHONPATH="."
DEST="//home/files"
YT="yt/wrapper/bin/yt/yt"
UBUNTU_VERSION="$(lsb_release --short --codename )"
VERSION=$(dpkg-parsechangelog | grep Version | awk '{print $2}')

silent_grep() {
    grep "$@" || true
}

make_link()
{
    src="$1"
    dst="$2"
    # TODO(ignat): make atomic
    $YT remove "$dst" --force
    $YT link "$src" "$dst"
}

urlencode() {
    echo "$@" | python2 -c "import sys, urllib; sys.stdout.write(urllib.quote(sys.stdin.read()))"
}

FILES=$($YT list $DEST)

# Upload python egg
FOUND_EGG=$(echo "$FILES" | silent_grep ${VERSION/-/_} | silent_grep ".egg\$")
if [ -z "$FOUND_EGG" ] || [ -n "$FORCE_DEPLOY" ]; then
    python setup.py bdist_egg
    EGG_FILEPATH=$(find dist/ -name "*.egg" | head -n 1)
    EGG_FILE="${EGG_FILEPATH##*/}"
    cat "$EGG_FILEPATH" | $YT upload "$DEST/$EGG_FILE"
    make_link "$DEST/$EGG_FILE" "$DEST/yandex-yt.egg"
fi

PYTHON_VERSION="$(python --version | awk '{print $2}')"

# If we use python2.7 then...
# NB: pyinstaller does not work correctly with python <= 2.6.
if [ "$PYTHON_VERSION" != "${PYTHON_VERSION/2.7/}" ]; then
    # Upload self-contained binaries
    for name in yt mapreduce-yt yt-fuse; do
        if [ -n "$(echo "$FILES" | silent_grep "${name}_${VERSION}_${UBUNTU_VERSION}")" ] && [ -z "$FORCE_DEPLOY" ]; then
            continue
        fi
        rm -rf build dist
        pyinstaller/pyinstaller.py --noconfirm --onefile yt/wrapper/bin/$name/$name
        pyinstaller/pyinstaller.py --noconfirm "${name}.spec"
        cat dist/$name | $YT upload "$DEST/${name}_${VERSION}_${UBUNTU_VERSION}"
        make_link "$DEST/${name}_${VERSION}_${UBUNTU_VERSION}" "$DEST/${name}_${UBUNTU_VERSION}"
    done
fi


if [ -n "$CREATE_CONDUCTOR_TICKET" ]; then
    HEADER="Cookie: conductor_auth=419fb75155c27d44f1d110ec833400fa"
    RESULT=$(curl --verbose --show-error \
        -H "$HEADER" \
        "http://c.yandex-team.ru/api/generator/package_version_ticket?package=yandex-yt-python&version=$VERSION&branch=unstable")
    PYTHON_SCRIPT='import json, sys; sys.stdout.write(str("unstable" in json.loads('"'${RESULT}'"')))'
    IS_TICKET_EXISTS="$(python -c "$PYTHON_SCRIPT")"
    if [ "$IS_TICKET_EXISTS" = "False" ]; then
        # Create ticket in coductor
        COMMENT=$(urlencode $(dpkg-parsechangelog))
        curl --verbose --show-error \
            -H "$HEADER" \
            "http://c.yandex-team.ru/auth_update/ticket_add?package\[0\]=yandex-yt-python&version\[0\]=$VERSION&ticket\[branch\]=unstable&ticket\[comment\]=$COMMENT"
    fi
fi
