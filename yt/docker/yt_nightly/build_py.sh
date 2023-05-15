#!/bin/bash

set -e

pip3 install wheel

script_name=$0
ytsaurus_source_path="."
ytsaurus_build_path="."

if [[ $# -eq 0 ]]; then
    print_usage
fi

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --ytsaurus-source-path)
        ytsaurus_source_path=$(realpath "$2")
        shift 2
        ;;
        --ytsaurus-build-path)
        ytsaurus_build_path=$(realpath "$2")
        shift 2
        ;;
        *)
        echo "Unknown argument $1"
        print_usage
        ;;
    esac
done

ytsaurus_python=$(realpath "${ytsaurus_build_path}/ytsaurus_python")

mkdir -p ${ytsaurus_python}
cd ${ytsaurus_source_path}
pip3 install -e yt/python/packages

$ytsaurus_source_path/yt/python/packages/yt_setup/generate_python_proto.py \
    --source-root ${ytsaurus_source_path} \
    --output ${ytsaurus_python}


cd $ytsaurus_source_path/yt/python/packages
python3 -m yt_setup.prepare_python_modules \
    --source-root ${ytsaurus_source_path} \
    --build-root ${ytsaurus_build_path} \
    --output-path ${ytsaurus_python} \
    --prepare-bindings-libraries

cp ${ytsaurus_source_path}/yt/python/packages/ytsaurus-native-driver/setup.py ${ytsaurus_python}

cd ${ytsaurus_python}
python3 setup.py bdist_wheel --py-limited-api cp34
