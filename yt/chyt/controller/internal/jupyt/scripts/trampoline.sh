#!/bin/bash

if [ "$PYTHON" == "" ] ; then 
    echo "There must be a PYTHON env variable to run trampoline" >&2
    exit 1
fi

if [ -e venv.tgz ] ; then
    echo "Unpacking venv" >&2
    tar xzf venv.tgz >&2
    pushd venv
    venv_root=$(pwd)
    echo "Patching venv with VENVROOT=$venv_root" >&2
    for f in $(fgrep -r VENVROOT -l) ; do
        sed -s "s|VENVROOT|$venv_root|g" -i $f
    done
    echo "Activating venv" >&2
    source bin/activate
    popd
fi

$PYTHON --version >&2

mkdir jupyter_conf

echo "Generating and patching config" >&2

export JUPYTER_CONFIG_DIR="./jupyter_conf"
$PYTHON -m jupyterlab --generate-config
cat >>./jupyter_conf/jupyter_lab_config.py <<EOF
c.NotebookApp.ip = '::'
c.NotebookApp.allow_origin = '*'
c.NotebookApp.disable_check_xsrf = True
EOF

echo "Running jupyter lab" >&2
mkdir sandbox
pushd sandbox
$PYTHON -m jupyterlab
