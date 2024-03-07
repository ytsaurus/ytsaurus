#!/bin/bash -e

pip3 install wheel auditwheel patchelf

script_name=$0
ytsaurus_source_path="."
ytsaurus_build_path="."
ytsaurus_package_name=""
prepare_bindings_libraries=true
apply_auditwheel=false

print_usage() {
    cat << EOF
Usage: $script_name [-h|--help]
                    [--ytsaurus-source-path /path/to/ytsaurus.repo (default: $ytsaurus_source_path)]
                    [--ytsaurus-build-path /path/to/ytsaurus.build (default: $ytsaurus_build_path)]
                    [--ytsaurus-package-name some-ytsaurus-package-name (default: all packages will be build) (values: ytsaurus-client, ytsaurus-yson, ytsaurus-local, ytsaurus-native-driver)]
                    [--not-prepare-bindings-libraries]
                    [--apply-auditwheel]
EOF
    exit 1
}

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
        --ytsaurus-package-name)
        ytsaurus_package_name=$2
        shift 2
        ;;
        --not-prepare-bindings-libraries)
        prepare_bindings_libraries=false
        shift 2
        ;;
        --apply-auditwheel)
        apply_auditwheel=true
        shift 1
        ;;
        *)
        echo "Unknown argument $1"
        print_usage
        ;;
    esac
done

set -u

ytsaurus_python=$(realpath "${ytsaurus_build_path}/ytsaurus_python")

mkdir -p ${ytsaurus_python}
cd ${ytsaurus_source_path}
pip3 install -e yt/python/packages

$ytsaurus_source_path/yt/python/packages/yt_setup/generate_python_proto.py \
    --source-root ${ytsaurus_source_path} \
    --output ${ytsaurus_python}


cd $ytsaurus_source_path/yt/python/packages

if [[ "$prepare_bindings_libraries" = true ]]; then
    prepare_bindings_libraries_option="--prepare-bindings-libraries"
else
    prepare_bindings_libraries_option=""
fi

python3 -m yt_setup.prepare_python_modules \
    --source-root ${ytsaurus_source_path} \
    --build-root ${ytsaurus_build_path} \
    --output-path ${ytsaurus_python} \
    $prepare_bindings_libraries_option

cd ${ytsaurus_python}

if [[ ${ytsaurus_package_name} == "" ]]; then
    packages=("ytsaurus-client" "ytsaurus-yson" "ytsaurus-local" "ytsaurus-native-driver")
else
    packages=("${ytsaurus_package_name}")
fi

for package in ${packages[@]}; do
    cp "${ytsaurus_source_path}/yt/python/packages/${package}/setup.py" .

    package_undescored=$(echo -e $package | sed -e s/-/_/g)
    dist_dir="${package_undescored}_dist"

    if [[ ${package} == "ytsaurus-native-driver" ]] || [[ ${package} == "ytsaurus-yson" ]]; then
        python3 setup.py bdist_wheel --py-limited-api cp34 --dist-dir ${dist_dir}
        if [[ ${apply_auditwheel} == "true" ]]; then
            for wheel in ${dist_dir}/${package_undescored}*.whl; do
                auditwheel repair "$wheel" -w "${dist_dir}" --plat manylinux2014_x86_64
                # Remove original wheel.
                rm "$wheel"
            done
        fi
    else
        python3 setup.py bdist_wheel --universal --dist-dir ${dist_dir}
    fi
done

