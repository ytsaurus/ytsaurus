PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/wrapper/testlib)
ELSE()
    PEERDIR(
        yt/python/yt/wrapper
        yt/python/yt/testlib
        yt/python/yt/yson
        yt/python/yt/environment/components/query_tracker

        library/python/resource
    )

    RESOURCE(
        yt/python/yt/wrapper/bin/yt /binaries/yt
        yt/python/yt/wrapper/bin/mapreduce-yt /binaries/mapreduce-yt
    )

    PY_SRCS(
        NAMESPACE yt.wrapper.testlib

        __init__.py
        helpers_cli.py
        conftest_helpers.py
        helpers.py
    )
ENDIF()

END()
