PY3_PROGRAM(deploy_python_package)

COPY_FILE(
    ${ARCADIA_ROOT}/yt/teamcity-build/python/python_packaging/pypi_helpers.py
    pypi_helpers.py
)

PY_SRCS(
    NAMESPACE yt_setup
    __init__.py
    convert_changelog_to_rst.py
    deploy.py
    MAIN doall.py
    find_pypi_package.py
    find_debian_package.py
    generate_python_proto.py
    pypi_helpers.py
    helpers.py
    os_helpers.py
    prepare_python_modules.py
)

PEERDIR(
    contrib/python/setuptools
    contrib/python/requests
)

END()
