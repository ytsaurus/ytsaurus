#!/bin/bash

PYTHON=$1
if [ "$1" == "" ] ; then
    echo "Specify python executable with minor version as a single line argument, e.g. python3.10" >&2
    exit 1
fi

echo "Creating venv with $PYTHON" >&2 
rm -rf venv
$PYTHON -B -m venv venv
pushd venv
venv_root=$(pwd)

echo "Installing jupyter lab" >&2
(
    source bin/activate
    pip install jupyterlab
)

echo "Removing pycache" >&2
find -name __pycache__ | xargs rm -r

echo "Replacing venv root with VENVROOT placeholder"
for f in $(fgrep -r "$venv_root" -l) ; do
    echo "Patching $f" >&2
    sed -s "s|$venv_root|VENVROOT|g" -i $f
done

popd

arch="venv.$(basename $PYTHON).tgz"
rm -f $arch
tar czf $arch venv/
