#!/usr/bin/env bash

# This script is intended to reupload Sandbox resource with different name.
# Renaming is needed to prevent name clashing between all Ya make outputs in one artifact directory.
# Remove this script after DEVTOOLS-4388.

set -e

resource_id="$1"
resource_name="$2"

if [ -z "$resource_id" ];
then
    echo "Specify resource id as a first argument"
    exit 1
fi

if [ -z "$resource_name" ];
then
    echo "Specify resource name as a second argument"
    exit 1
fi

if [ -e "$resource_name" ];
then
    echo "Local file $resource_name is already exists"
    exit 1
fi

download_url=https://proxy.sandbox.yandex-team.ru/"$resource_id"
echo "Downloading resource from" "$download_url"
curl -vvv "$download_url" > "$resource_name"

echo

echo "Uploading resource" "$resource_name"
ya upload "$resource_name" \
    --ttl inf \
    -d "Renamed copy of the Sandbox resource #$resource_id" \
    --do-not-remove \
    --sandbox \
    --skynet
