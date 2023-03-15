set -euxo pipefail

version=`ya tool jq -r '.meta.version' pkg.json`

# check python version
python -c 'import sys; assert sys.version_info[0] == 2, "python has to be an alias to python2 in order to build py2 package using ya package command"'

for python_major_version in 2 3
do
    flags=
    if [[ $python_major_version == "3" ]]
    then
        flags="$flags --wheel-python3"
    fi
    ya package \
        $flags \
        --wheel pkg.json
    twine upload -r yandex "yandex_type_info-${version}-py${python_major_version}-none-any.whl"
done
