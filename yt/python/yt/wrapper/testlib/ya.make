PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/python/yt/wrapper
    yt/python/yt/testlib
    yt/python/yt/yson

    library/python/resource
)

RESOURCE(
    yt/python/yt/wrapper/bin/yt /binaries/yt
    yt/python/yt/wrapper/bin/mapreduce-yt /binaries/mapreduce-yt
)

PY_SRCS(
    NAMESPACE yt.wrapper.testlib

    __init__.py
    conftest_helpers.py
    helpers.py
)

END()
