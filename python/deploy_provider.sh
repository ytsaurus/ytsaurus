#!/bin/bash -eux

YT_PYTHON="../../python"

PATHS="convert_changelog_to_rst.py deploy.sh helpers.py find_package.py build_helpers"
    
trap "{ rm -rf $PATHS; }" EXIT

for FILE in $PATHS; do
    cp -r "$YT_PYTHON/$FILE" .
done

./deploy.sh $@

